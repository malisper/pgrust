#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! Shared PostgreSQL node-struct *types* for the node infrastructure.
//!
//! This crate is the common vocabulary that `backend-nodes-copyfuncs`
//! (`copyObjectImpl`) and `backend-nodes-equalfuncs` (`equal`) will both build
//! on. It defines the node structs as real PostgreSQL ABI types and the
//! complete `NodeTag` constant table, while reusing the `NodeTag` *type*,
//! `List`, `Bitmapset`, and the value nodes (`Integer`, `Float`, `Boolean`,
//! `StringNode`/`String`, `BitString`) from `pgrust-pg-ffi` rather than
//! redefining them.
//!
//! # Scope
//!
//! Every struct in this crate is `#[repr(C)]` because node trees are part of
//! the PostgreSQL backend ABI: they are allocated with `palloc` in a
//! [`MemoryContext`](mmgr_fgram) and deep-copied by `copyObject`, so
//! their layout must match the C backend exactly. The crate currently models:
//!
//! - the full `NodeTag` table ([`node_tags`]),
//! - the primitive expression family ([`primnodes`]): `Var`, `Const`,
//!   `Param`, `Aggref`, `WindowFunc`, `FuncExpr`, `OpExpr` (and its
//!   `DistinctExpr`/`NullIfExpr` aliases), `ScalarArrayOpExpr`, `BoolExpr`,
//!   `SubLink`, the coercion nodes, `CaseExpr`/`CaseWhen`/`CaseTestExpr`,
//!   `ArrayExpr`, `RowExpr`, `CoalesceExpr`, `MinMaxExpr`, `NullTest`,
//!   `BooleanTest`, `SetToDefault`, `CurrentOfExpr`, `NextValueExpr`,
//!   `TargetEntry`, and the join-tree nodes `RangeTblRef`/`JoinExpr`/
//!   `FromExpr`/`OnConflictExpr`,
//! - the core parse-tree nodes ([`parsenodes`]): `Query`, `RangeTblEntry`,
//!   `SortGroupClause`, `RowMarkClause`, `WithClause`, `CommonTableExpr`,
//!   `AppendRelInfo`,
//! - the raw DML statement family ([`parsenodes_stmts`]) and the DDL / utility
//!   statement family ([`parsenodes_ddl`]) - scaffolded for the Structs phase,
//! - the plan-tree base nodes ([`plannodes`]): `Plan`, `Scan`, `Join`,
//!   `SeqScan`, `ModifyTableHeader`,
//! - `Alias`, `RangeVar`, `TableFunc`, `IntoClause` here in the crate root.
//!
//! # Coverage aggregation
//!
//! Each family module exports `node_types_covered()` returning the node types
//! it models. [`coverage_families`] lists those slices in declaration order and
//! [`coverage_iter`] concatenates them into the crate-wide view; the Structs
//! phase grows a family by editing only that module's file - no `lib.rs` change
//! is needed when new structs land.
//!
//! # The ABI seam ([`OpaqueNode`])
//!
//! Node variants whose members point at catalog/executor/planner structs that
//! are not ported yet keep those members as raw pointers to [`OpaqueNode`]
//! (an ABI-compatible opaque `Node`-headed type). This is the documented seam:
//! the field still occupies a single pointer slot (matching the C ABI), but
//! the pointee type is intentionally not committed to until the dependency is
//! ported. The deferred pointees are listed in [`DEFERRED_NODE_SEAMS`].
//!
//! # Memory ownership
//!
//! These are *type definitions only*. Node trees are owned by PostgreSQL
//! memory contexts: `copyObject` palloc-copies a tree into
//! `CurrentMemoryContext`, and the tree is freed by resetting/deleting that
//! context, never by Rust `Drop`. The eventual `copyObjectImpl`/`equal` will
//! allocate through [`mmgr_fgram`] (`palloc`/`MemoryContextScope`) and
//! must not introduce `Box`/`Vec`/`Drop` ownership for node storage.

use core::ffi::{c_char, c_int};

use ::pg_ffi_fgram::{Bitmapset, List, NodeTag};

pub mod execnodes;
pub mod node_tags;
pub mod parsenodes;
pub mod parsenodes_ddl;
pub mod parsenodes_stmts;
pub mod pathnodes;
pub mod plannodes;
pub mod primnodes;

// Reuse the value nodes and shared containers from the FFI crate; never
// redefine `NodeTag`, `List`, `Bitmapset`, or the value nodes here.
pub use ::pg_ffi_fgram::{
    BitString, Boolean, Float, Integer, Node, NodeTag as NodeTagType, StringNode,
};

pub use execnodes::IndexInfo;
pub use node_tags::*;
pub use parsenodes::{
    AppendRelInfo, CommonTableExpr, Query, RangeTblEntry, RowMarkClause, SortGroupClause,
    WithClause,
};
pub use pathnodes::{
    get_extensible_node_methods, set_extensible_node_methods_resolver, ExtensibleNode,
    ExtensibleNodeMethods, ExtensibleNodeMethodsResolver, ExtensibleNodeMethodsResult,
    ForeignKeyCacheInfo, GroupByOrdering, PathKey, PlaceHolderInfo, PlaceHolderVar, QualCost,
    RestrictInfo, SpecialJoinInfo,
};
pub use plannodes::{Join, ModifyTableHeader, Plan, Scan, SeqScan};
pub use primnodes::{
    Aggref, ArrayCoerceExpr, ArrayExpr, BoolExpr, BooleanTest, CaseExpr, CaseTestExpr, CaseWhen,
    CoalesceExpr, CoerceToDomain, CoerceViaIO, Const, CurrentOfExpr, DistinctExpr, Expr, FromExpr,
    FuncExpr, GroupingFunc, JoinExpr, MinMaxExpr, NamedArgExpr, NextValueExpr, NullIfExpr,
    NullTest, OnConflictExpr, OpExpr, Param, RangeTblRef, RangeVar, RelabelType, RowExpr,
    ScalarArrayOpExpr, SetToDefault, SubLink, TargetEntry, Var, WindowFunc,
};

use primnodes::ParseLoc;

/// `String` value node, spelled the PostgreSQL way; an alias for the FFI
/// [`StringNode`].
pub type String_ = StringNode;

pub type TableFuncType = core::ffi::c_uint;
pub const TFT_XMLTABLE: TableFuncType = 0;
pub const TFT_JSON_TABLE: TableFuncType = 1;

/// `Alias` - alias for a range variable.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Alias {
    pub type_: NodeTag,
    pub aliasname: *mut c_char,
    pub colnames: *mut List,
}

/// `TableFunc` - XMLTABLE / JSON_TABLE table function node.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableFunc {
    pub type_: NodeTag,
    pub functype: TableFuncType,
    pub ns_uris: *mut List,
    pub ns_names: *mut List,
    pub docexpr: *mut Node,
    pub rowexpr: *mut Node,
    pub colnames: *mut List,
    pub coltypes: *mut List,
    pub coltypmods: *mut List,
    pub colcollations: *mut List,
    pub colexprs: *mut List,
    pub coldefexprs: *mut List,
    pub colvalexprs: *mut List,
    pub passingvalexprs: *mut List,
    pub notnulls: *mut Bitmapset,
    pub plan: *mut Node,
    pub ordinalitycol: c_int,
    pub location: ParseLoc,
}

/// ABI seam: an opaque, `Node`-headed placeholder for pointees of node types
/// that are not modelled yet (catalog/executor/planner-internal structs).
///
/// A `*mut OpaqueNode` field is layout-identical to the C `SomeType *` member
/// it stands in for (one pointer), so structs that embed it keep the correct
/// ABI. Reading past the leading [`NodeTag`] is undefined until the concrete
/// type is ported; treat it as a forward declaration.
#[repr(C)]
pub struct OpaqueNode {
    pub type_: NodeTag,
    _opaque: [u8; 0],
}

impl OpaqueNode {
    /// The node tag of an opaque pointee, if the pointer is non-null.
    ///
    /// # Safety
    /// `ptr` must be null or point at a live, `palloc`-allocated node whose
    /// first field is a `NodeTag`.
    pub unsafe fn node_tag(ptr: *const OpaqueNode) -> Option<NodeTag> {
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { (*ptr).type_ })
        }
    }
}

/// How a node type is currently represented in this crate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeTypeCoverage {
    /// Defined as a full `#[repr(C)]` struct here.
    Modelled,
    /// Reused from `pgrust-pg-ffi` (value node or shared container).
    ReusedFromFfi,
    /// Reachable only through the [`OpaqueNode`] seam (pointee not ported).
    SeamDeferred,
}

/// One entry describing how a node type is covered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeTypeStatus {
    pub name: &'static str,
    pub tag: NodeTag,
    pub coverage: NodeTypeCoverage,
}

/// Root-level coverage: the value nodes / shared containers reused from
/// `pgrust-pg-ffi`, plus the two node types defined directly in this crate root
/// (`Alias`, `TableFunc`). The per-family structs are registered by each family
/// module's `node_types_covered()` and folded in by [`coverage_families`].
pub fn root_node_types_covered() -> &'static [NodeTypeStatus] {
    &[
        // Value nodes + containers reused from pgrust-pg-ffi.
        NodeTypeStatus {
            name: "List",
            tag: T_List,
            coverage: NodeTypeCoverage::ReusedFromFfi,
        },
        NodeTypeStatus {
            name: "IntList",
            tag: T_IntList,
            coverage: NodeTypeCoverage::ReusedFromFfi,
        },
        NodeTypeStatus {
            name: "OidList",
            tag: T_OidList,
            coverage: NodeTypeCoverage::ReusedFromFfi,
        },
        NodeTypeStatus {
            name: "XidList",
            tag: T_XidList,
            coverage: NodeTypeCoverage::ReusedFromFfi,
        },
        NodeTypeStatus {
            name: "Bitmapset",
            tag: T_Bitmapset,
            coverage: NodeTypeCoverage::ReusedFromFfi,
        },
        NodeTypeStatus {
            name: "Integer",
            tag: T_Integer,
            coverage: NodeTypeCoverage::ReusedFromFfi,
        },
        NodeTypeStatus {
            name: "Float",
            tag: T_Float,
            coverage: NodeTypeCoverage::ReusedFromFfi,
        },
        NodeTypeStatus {
            name: "Boolean",
            tag: T_Boolean,
            coverage: NodeTypeCoverage::ReusedFromFfi,
        },
        NodeTypeStatus {
            name: "String",
            tag: T_String,
            coverage: NodeTypeCoverage::ReusedFromFfi,
        },
        NodeTypeStatus {
            name: "BitString",
            tag: T_BitString,
            coverage: NodeTypeCoverage::ReusedFromFfi,
        },
        // Node types defined directly in this crate root.
        NodeTypeStatus {
            name: "Alias",
            tag: T_Alias,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "TableFunc",
            tag: T_TableFunc,
            coverage: NodeTypeCoverage::Modelled,
        },
    ]
}

/// The per-family coverage slices, in declaration order. Each family module
/// owns its own slice via `node_types_covered()`; the Structs phase grows a
/// family by editing only that module's file. `lib.rs` concatenates these into
/// the crate-wide coverage view ([`coverage_iter`] / [`NODE_TYPES_COVERED`])
/// without further wiring.
pub fn coverage_families() -> [&'static [NodeTypeStatus]; 7] {
    [
        root_node_types_covered(),
        primnodes::node_types_covered(),
        parsenodes::node_types_covered(),
        parsenodes_stmts::node_types_covered(),
        parsenodes_ddl::node_types_covered(),
        plannodes::node_types_covered(),
        pathnodes::node_types_covered(),
    ]
}

/// Flattened iterator over every node type modelled or reused by this crate -
/// the concatenation of all [`coverage_families`]. This is the set the eventual
/// `copyObjectImpl`/`equal` can already dispatch over without touching the seam.
pub fn coverage_iter() -> impl Iterator<Item = &'static NodeTypeStatus> {
    coverage_families().into_iter().flatten()
}

/// Total number of node types currently covered (modelled or reused), summed
/// across all families.
pub fn node_types_covered_count() -> usize {
    coverage_families().iter().map(|f| f.len()).sum()
}

/// Root + FFI-reused coverage entries, kept as a slice for backwards
/// compatibility. The complete, all-families view is [`coverage_iter`]; prefer
/// that when enumerating everything this crate covers.
pub const NODE_TYPES_COVERED: &[NodeTypeStatus] = &[
    NodeTypeStatus {
        name: "List",
        tag: T_List,
        coverage: NodeTypeCoverage::ReusedFromFfi,
    },
    NodeTypeStatus {
        name: "IntList",
        tag: T_IntList,
        coverage: NodeTypeCoverage::ReusedFromFfi,
    },
    NodeTypeStatus {
        name: "OidList",
        tag: T_OidList,
        coverage: NodeTypeCoverage::ReusedFromFfi,
    },
    NodeTypeStatus {
        name: "XidList",
        tag: T_XidList,
        coverage: NodeTypeCoverage::ReusedFromFfi,
    },
    NodeTypeStatus {
        name: "Bitmapset",
        tag: T_Bitmapset,
        coverage: NodeTypeCoverage::ReusedFromFfi,
    },
    NodeTypeStatus {
        name: "Integer",
        tag: T_Integer,
        coverage: NodeTypeCoverage::ReusedFromFfi,
    },
    NodeTypeStatus {
        name: "Float",
        tag: T_Float,
        coverage: NodeTypeCoverage::ReusedFromFfi,
    },
    NodeTypeStatus {
        name: "Boolean",
        tag: T_Boolean,
        coverage: NodeTypeCoverage::ReusedFromFfi,
    },
    NodeTypeStatus {
        name: "String",
        tag: T_String,
        coverage: NodeTypeCoverage::ReusedFromFfi,
    },
    NodeTypeStatus {
        name: "BitString",
        tag: T_BitString,
        coverage: NodeTypeCoverage::ReusedFromFfi,
    },
    NodeTypeStatus {
        name: "Alias",
        tag: T_Alias,
        coverage: NodeTypeCoverage::Modelled,
    },
    NodeTypeStatus {
        name: "TableFunc",
        tag: T_TableFunc,
        coverage: NodeTypeCoverage::Modelled,
    },
];

/// Abstract base plan structs that have no `NodeTag` of their own but are
/// modelled so concrete plan nodes can embed them.
pub const PLAN_BASE_STRUCTS: &[&str] = &["Plan", "Scan", "Join"];

/// Node types still left behind the [`OpaqueNode`] seam. Per the seam rule, an
/// entry is legitimate only when copyfuncs/equalfuncs do NOT traverse the
/// pointee: the planner pathnode / optimizer-internal and executor-state node
/// types, which the copy/equal layer never recurses into. Every node that
/// copy/equal *does* touch (the `Query`/parse-tree and `Plan` families, plus
/// the clause nodes such as `IntoClause.viewQuery`, `SampleScan.tablesample`,
/// `RangeTblEntry.tablesample`, `CommonTableExpr.search_clause`/`cycle_clause`,
/// and the `Query.utility_stmt`/`set_operations` `Node*` trees) is modelled as
/// a real `Node*` or concrete struct pointer and is NOT listed here.
///
/// These are recorded as type categories rather than individual struct fields
/// because no modelled, copy/equal-traversed struct embeds a pointer to them;
/// they appear only behind plannodes path/optimizer members that are themselves
/// out of the copy/equal scope.
pub const DEFERRED_NODE_SEAMS: &[(&str, &str)] = &[
    (
        "pathnodes (Path / RelOptInfo family)",
        "planner-internal: RelOptInfo, Path, EquivalenceClass, PathKey, RestrictInfo",
    ),
    (
        "executor state (PlanState family)",
        "executor-internal: EState, PlanState and friends",
    ),
];

/// Compile-time invariant the future copy/equal layers rely on: an `Expr`
/// subtype must begin with the [`Expr`] header so `((Expr *) node)->type`
/// resolves the node tag.
const _: () = {
    assert!(core::mem::offset_of!(Var, xpr) == 0);
    assert!(core::mem::offset_of!(OpExpr, xpr) == 0);
    assert!(core::mem::offset_of!(TargetEntry, xpr) == 0);
    assert!(core::mem::offset_of!(Scan, plan) == 0);
    assert!(core::mem::offset_of!(Join, plan) == 0);
};

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn expr_header_is_just_a_node_tag() {
        assert_eq!(size_of::<Expr>(), size_of::<NodeTag>());
        assert_eq!(align_of::<Expr>(), align_of::<NodeTag>());
    }

    #[test]
    fn var_layout_matches_postgres_abi() {
        // Expr(4) + pad(4) -> varno@? Var begins with xpr at 0.
        assert_eq!(offset_of!(Var, xpr), 0);
        assert_eq!(offset_of!(Var, varno), 4);
        assert_eq!(offset_of!(Var, varattno), 8);
        assert_eq!(offset_of!(Var, vartype), 12);
        assert_eq!(offset_of!(Var, vartypmod), 16);
        assert_eq!(offset_of!(Var, varcollid), 20);
        assert_eq!(offset_of!(Var, varnullingrels), 24);
        assert_eq!(offset_of!(Var, location), 48);
        assert_eq!(size_of::<Var>(), 56);
        assert_eq!(align_of::<Var>(), 8);
    }

    #[test]
    fn const_layout_matches_postgres_abi() {
        assert_eq!(offset_of!(Const, xpr), 0);
        assert_eq!(offset_of!(Const, consttype), 4);
        assert_eq!(offset_of!(Const, constvalue), 24);
        assert_eq!(offset_of!(Const, constisnull), 32);
        assert_eq!(offset_of!(Const, constbyval), 33);
        assert_eq!(offset_of!(Const, location), 36);
        assert_eq!(size_of::<Const>(), 40);
    }

    #[test]
    fn opexpr_layout_matches_postgres_abi() {
        assert_eq!(offset_of!(OpExpr, opno), 4);
        assert_eq!(offset_of!(OpExpr, args), 32);
        assert_eq!(offset_of!(OpExpr, location), 40);
        assert_eq!(size_of::<OpExpr>(), 48);
    }

    #[test]
    fn plan_base_is_first_field_of_scan_and_join() {
        assert_eq!(offset_of!(Scan, plan), 0);
        assert_eq!(offset_of!(Join, plan), 0);
        assert_eq!(offset_of!(SeqScan, scan), 0);
    }

    #[test]
    fn opaque_seam_is_one_pointer_wide_node_header() {
        // The seam pointee must be a Node-headed type so its tag is readable.
        assert_eq!(offset_of!(OpaqueNode, type_), 0);
        assert_eq!(size_of::<*mut OpaqueNode>(), size_of::<*mut Node>());
    }

    #[test]
    fn coverage_table_tags_are_distinct_and_nonzero_where_expected() {
        // Walk every family via the aggregated view, not just the root slice.
        for entry in coverage_iter() {
            // T_List is the only modelled/reused tag with value 1; none are
            // T_Invalid(0).
            assert_ne!(entry.tag, T_Invalid, "{} mapped to T_Invalid", entry.name);
        }
    }

    #[test]
    fn coverage_aggregation_concatenates_all_families() {
        // The aggregated count is the sum of every family's slice length, and
        // the iterator yields exactly that many entries.
        let summed: usize = coverage_families().iter().map(|f| f.len()).sum();
        assert_eq!(summed, node_types_covered_count());
        assert_eq!(coverage_iter().count(), node_types_covered_count());
        // Root + FFI entries are a non-empty subset of the whole.
        assert!(!NODE_TYPES_COVERED.is_empty());
        assert!(node_types_covered_count() >= NODE_TYPES_COVERED.len());
    }

    #[test]
    fn deferred_seams_are_recorded() {
        assert!(!DEFERRED_NODE_SEAMS.is_empty());
        for (field, _ctype) in DEFERRED_NODE_SEAMS {
            assert!(!field.is_empty());
        }
    }

    #[test]
    fn coverage_has_no_duplicate_name_or_tag_entries() {
        // Each node type must be registered in exactly one family's
        // node_types_covered() slice. A node defined in two modules (and
        // registered in both) would otherwise be double-counted under the same
        // (name, tag), and the two Rust definitions could silently diverge in
        // field names. This guards the dedup of the eight clause structs that
        // used to live in both parsenodes and parsenodes_stmts. (no_std: an
        // O(n^2) walk over the family slices avoids needing a heap collection.)
        let families = coverage_families();
        // Flatten by indexing so we can compare every entry against later ones
        // without allocating a Vec.
        let count = node_types_covered_count();
        let nth = |k: usize| -> &NodeTypeStatus {
            families.iter().flat_map(|f| f.iter()).nth(k).unwrap()
        };
        for i in 0..count {
            let a = nth(i);
            for j in (i + 1)..count {
                let b = nth(j);
                assert_ne!(
                    a.name, b.name,
                    "node type {} is registered in more than one coverage slice",
                    a.name
                );
                assert_ne!(
                    a.tag, b.tag,
                    "tag for {} / {} is registered twice",
                    a.name, b.name
                );
            }
        }
    }
}
