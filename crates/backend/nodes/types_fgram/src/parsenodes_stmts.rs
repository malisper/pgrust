//! Raw-statement parse nodes (`nodes/parsenodes.h`): the optimizable DML and
//! query statement family produced by the grammar before analysis -
//! `SelectStmt`, `InsertStmt`, `UpdateStmt`, `DeleteStmt`, `MergeStmt`,
//! `SetOperationStmt`, `RawStmt` - together with the raw-grammar expression,
//! target, and clause helper nodes they hang off of (`ResTarget`, `ColumnRef`,
//! `A_Expr`, `A_Const`, `FuncCall`, `TypeName`, `SortBy`, `WindowDef`,
//! `RangeSubselect`, ...).
//!
//! Every struct here is a real PostgreSQL ABI type, so it is `#[repr(C)]` with
//! field order, names, types, and widths matching the C backend (cross-checked
//! against `nodes/parsenodes.h` and the c2rust copyfuncs embedded defs). These
//! are the raw-parse-tree counterparts of the analyzed nodes in
//! [`crate::parsenodes`]; `copyfuncs`/`equalfuncs` traverse all of them, so
//! none of them sit behind the [`crate::OpaqueNode`] seam. Sub-pointers to
//! sibling raw nodes use the concrete `*mut <Struct>` forward reference.
//!
//! `NodeTag`, `List`, and the value nodes (`Integer`, `Float`, `Boolean`,
//! `String`, `BitString`) are reused from `pgrust-pg-ffi`; `RangeVar`,
//! `IntoClause`, `Alias`, and `WithClause` are reused from the already-modelled
//! crate modules.

use core::ffi::{c_char, c_int};

use ::pg_ffi_fgram::{BitString, Boolean, Float, Integer, List, Node, NodeTag, Oid, StringNode};

use crate::primnodes::{CoercionForm, IntoClause, OverridingKind, ParseLoc, RangeVar};
use crate::{Alias, WithClause};

// ---------------------------------------------------------------------------
// Supporting enums (kept as `c_uint` for exact ABI match).
// ---------------------------------------------------------------------------

/// `A_Expr_Kind` - infix/prefix/postfix expression flavor (`A_Expr.kind`).
pub type A_Expr_Kind = core::ffi::c_uint;
pub const AEXPR_OP: A_Expr_Kind = 0;
pub const AEXPR_OP_ANY: A_Expr_Kind = 1;
pub const AEXPR_OP_ALL: A_Expr_Kind = 2;
pub const AEXPR_DISTINCT: A_Expr_Kind = 3;
pub const AEXPR_NOT_DISTINCT: A_Expr_Kind = 4;
pub const AEXPR_NULLIF: A_Expr_Kind = 5;
pub const AEXPR_IN: A_Expr_Kind = 6;
pub const AEXPR_LIKE: A_Expr_Kind = 7;
pub const AEXPR_ILIKE: A_Expr_Kind = 8;
pub const AEXPR_SIMILAR: A_Expr_Kind = 9;
pub const AEXPR_BETWEEN: A_Expr_Kind = 10;
pub const AEXPR_NOT_BETWEEN: A_Expr_Kind = 11;
pub const AEXPR_BETWEEN_SYM: A_Expr_Kind = 12;
pub const AEXPR_NOT_BETWEEN_SYM: A_Expr_Kind = 13;

/// `SortByDir` - ASC/DESC/USING/default for an ORDER BY item.
pub type SortByDir = core::ffi::c_uint;
pub const SORTBY_DEFAULT: SortByDir = 0;
pub const SORTBY_ASC: SortByDir = 1;
pub const SORTBY_DESC: SortByDir = 2;
pub const SORTBY_USING: SortByDir = 3;

/// `SortByNulls` - NULLS FIRST/LAST/default for an ORDER BY item.
pub type SortByNulls = core::ffi::c_uint;
pub const SORTBY_NULLS_DEFAULT: SortByNulls = 0;
pub const SORTBY_NULLS_FIRST: SortByNulls = 1;
pub const SORTBY_NULLS_LAST: SortByNulls = 2;

/// `GroupingSetKind` - the flavor of a `GroupingSet` node.
pub type GroupingSetKind = core::ffi::c_uint;
pub const GROUPING_SET_EMPTY: GroupingSetKind = 0;
pub const GROUPING_SET_SIMPLE: GroupingSetKind = 1;
pub const GROUPING_SET_ROLLUP: GroupingSetKind = 2;
pub const GROUPING_SET_CUBE: GroupingSetKind = 3;
pub const GROUPING_SET_SETS: GroupingSetKind = 4;

/// `OnConflictAction` - DO NOTHING / DO UPDATE selector.
pub type OnConflictAction = core::ffi::c_uint;
pub const ONCONFLICT_NONE: OnConflictAction = 0;
pub const ONCONFLICT_NOTHING: OnConflictAction = 1;
pub const ONCONFLICT_UPDATE: OnConflictAction = 2;

/// `MergeMatchKind` - MATCHED / NOT MATCHED BY SOURCE / NOT MATCHED BY TARGET.
pub type MergeMatchKind = core::ffi::c_uint;
pub const MERGE_WHEN_MATCHED: MergeMatchKind = 0;
pub const MERGE_WHEN_NOT_MATCHED_BY_SOURCE: MergeMatchKind = 1;
pub const MERGE_WHEN_NOT_MATCHED_BY_TARGET: MergeMatchKind = 2;

/// `ReturningOptionKind` - OLD/NEW alias option in `RETURNING WITH(...)`.
pub type ReturningOptionKind = core::ffi::c_uint;
pub const RETURNING_OPTION_OLD: ReturningOptionKind = 0;
pub const RETURNING_OPTION_NEW: ReturningOptionKind = 1;

/// `LimitOption` - LIMIT count semantics (mirrors `parsenodes::LimitOption`).
pub type LimitOption = core::ffi::c_uint;
pub const LIMIT_OPTION_COUNT: LimitOption = 0;
pub const LIMIT_OPTION_WITH_TIES: LimitOption = 1;

/// `SetOperation` - UNION/INTERSECT/EXCEPT set-operation kind.
pub type SetOperation = core::ffi::c_uint;
pub const SETOP_NONE: SetOperation = 0;
pub const SETOP_UNION: SetOperation = 1;
pub const SETOP_INTERSECT: SetOperation = 2;
pub const SETOP_EXCEPT: SetOperation = 3;

// ---------------------------------------------------------------------------
// Raw-grammar primitive / reference nodes.
// ---------------------------------------------------------------------------

/// `TypeName` - the raw representation of a SQL type name.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TypeName {
    pub type_: NodeTag,
    pub names: *mut List,
    pub typeOid: Oid,
    pub setof: bool,
    pub pct_type: bool,
    pub typmods: *mut List,
    pub typemod: i32,
    pub arrayBounds: *mut List,
    pub location: ParseLoc,
}

/// `ColumnRef` - a reference to a column (or whole tuple).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ColumnRef {
    pub type_: NodeTag,
    pub fields: *mut List,
    pub location: ParseLoc,
}

/// `ParamRef` - a `$n` parameter reference.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ParamRef {
    pub type_: NodeTag,
    pub number: c_int,
    pub location: ParseLoc,
}

/// `A_Expr` - an infix/prefix/postfix expression.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct A_Expr {
    pub type_: NodeTag,
    pub kind: A_Expr_Kind,
    pub name: *mut List,
    pub lexpr: *mut Node,
    pub rexpr: *mut Node,
    pub rexpr_list_start: ParseLoc,
    pub rexpr_list_end: ParseLoc,
    pub location: ParseLoc,
}

/// `union ValUnion` - the inline value-node payload of `A_Const`.
///
/// A union of the value nodes plus a bare `Node` header, matching the C
/// `union ValUnion`. The widest arm (`Float`/`String`/`BitString`, each a
/// `NodeTag` + pointer) fixes its size at 16 bytes / 8-byte alignment.
#[repr(C)]
#[derive(Clone, Copy)]
pub union ValUnion {
    pub node: Node,
    pub ival: Integer,
    pub fval: Float,
    pub boolval: Boolean,
    pub sval: StringNode,
    pub bsval: BitString,
}

/// `A_Const` - a literal constant, with its value node stored inline in `val`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct A_Const {
    pub type_: NodeTag,
    pub val: ValUnion,
    pub isnull: bool,
    pub location: ParseLoc,
}

/// `TypeCast` - a `CAST` expression.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TypeCast {
    pub type_: NodeTag,
    pub arg: *mut Node,
    pub typeName: *mut TypeName,
    pub location: ParseLoc,
}

/// `CollateClause` - a `COLLATE` expression.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollateClause {
    pub type_: NodeTag,
    pub arg: *mut Node,
    pub collname: *mut List,
    pub location: ParseLoc,
}

/// `FuncCall` - a function or aggregate invocation in the raw parse tree.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FuncCall {
    pub type_: NodeTag,
    pub funcname: *mut List,
    pub args: *mut List,
    pub agg_order: *mut List,
    pub agg_filter: *mut Node,
    pub over: *mut WindowDef,
    pub agg_within_group: bool,
    pub agg_star: bool,
    pub agg_distinct: bool,
    pub func_variadic: bool,
    pub funcformat: CoercionForm,
    pub location: ParseLoc,
}

/// `A_Star` - a `*` standing for all columns of a table or compound field.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct A_Star {
    pub type_: NodeTag,
}

/// `A_Indices` - array subscript or slice bounds (`[idx]` or `[lidx:uidx]`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct A_Indices {
    pub type_: NodeTag,
    pub is_slice: bool,
    pub lidx: *mut Node,
    pub uidx: *mut Node,
}

/// `A_Indirection` - select a field and/or array element from an expression.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct A_Indirection {
    pub type_: NodeTag,
    pub arg: *mut Node,
    pub indirection: *mut List,
}

/// `A_ArrayExpr` - an `ARRAY[]` construct.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct A_ArrayExpr {
    pub type_: NodeTag,
    pub elements: *mut List,
    pub list_start: ParseLoc,
    pub list_end: ParseLoc,
    pub location: ParseLoc,
}

/// `ResTarget` - a result-target / target-column entry in a raw parse tree.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ResTarget {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub indirection: *mut List,
    pub val: *mut Node,
    pub location: ParseLoc,
}

/// `MultiAssignRef` - element of a row-source expression for UPDATE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MultiAssignRef {
    pub type_: NodeTag,
    pub source: *mut Node,
    pub colno: c_int,
    pub ncolumns: c_int,
}

/// `SortBy` - one item of an `ORDER BY` clause.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SortBy {
    pub type_: NodeTag,
    pub node: *mut Node,
    pub sortby_dir: SortByDir,
    pub sortby_nulls: SortByNulls,
    pub useOp: *mut List,
    pub location: ParseLoc,
}

/// `WindowDef` - the raw representation of `WINDOW` and `OVER` clauses.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WindowDef {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub refname: *mut c_char,
    pub partitionClause: *mut List,
    pub orderClause: *mut List,
    pub frameOptions: c_int,
    pub startOffset: *mut Node,
    pub endOffset: *mut Node,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// Raw-grammar FROM-clause / range nodes.
// ---------------------------------------------------------------------------

/// `RangeSubselect` - a subquery appearing in a `FROM` clause.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RangeSubselect {
    pub type_: NodeTag,
    pub lateral: bool,
    pub subquery: *mut Node,
    pub alias: *mut Alias,
}

/// `RangeFunction` - a function call appearing in a `FROM` clause.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RangeFunction {
    pub type_: NodeTag,
    pub lateral: bool,
    pub ordinality: bool,
    pub is_rowsfrom: bool,
    pub functions: *mut List,
    pub alias: *mut Alias,
    pub coldeflist: *mut List,
}

/// `RangeTableFunc` - the raw form of table functions such as `XMLTABLE`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RangeTableFunc {
    pub type_: NodeTag,
    pub lateral: bool,
    pub docexpr: *mut Node,
    pub rowexpr: *mut Node,
    pub namespaces: *mut List,
    pub columns: *mut List,
    pub alias: *mut Alias,
    pub location: ParseLoc,
}

/// `RangeTableFuncCol` - one column in a [`RangeTableFunc`]'s column list.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RangeTableFuncCol {
    pub type_: NodeTag,
    pub colname: *mut c_char,
    pub typeName: *mut TypeName,
    pub for_ordinality: bool,
    pub is_not_null: bool,
    pub colexpr: *mut Node,
    pub coldefexpr: *mut Node,
    pub location: ParseLoc,
}

/// `RangeTableSample` - `TABLESAMPLE` appearing in a raw `FROM` clause.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RangeTableSample {
    pub type_: NodeTag,
    pub relation: *mut Node,
    pub method: *mut List,
    pub args: *mut List,
    pub repeatable: *mut Node,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// Grouping / merge / on-conflict / returning helper clauses.
// ---------------------------------------------------------------------------

// `GroupingSet`, `InferClause`, `OnConflictClause`, `MergeWhenClause`,
// `ReturningOption`, `ReturningClause`, `CTESearchClause`, and `CTECycleClause`
// are single-sourced in `parsenodes` (the canonical, snake-case definitions).
// They are re-exported here so statement structs in this module can reference
// them without redefining the (ABI-identical) types, which previously let the
// two copies double-count in the coverage table and drift on field names.
pub use crate::parsenodes::{
    CTECycleClause, CTESearchClause, GroupingSet, InferClause, MergeWhenClause, OnConflictClause,
    ReturningClause, ReturningOption,
};

// ---------------------------------------------------------------------------
// Top-level optimizable / raw statements.
// ---------------------------------------------------------------------------

/// `RawStmt` - the container for one statement's raw parse tree.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RawStmt {
    pub type_: NodeTag,
    pub stmt: *mut Node,
    pub stmt_location: ParseLoc,
    pub stmt_len: ParseLoc,
}

/// `InsertStmt` - an `INSERT` statement.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct InsertStmt {
    pub type_: NodeTag,
    pub relation: *mut RangeVar,
    pub cols: *mut List,
    pub selectStmt: *mut Node,
    pub onConflictClause: *mut OnConflictClause,
    pub returningClause: *mut ReturningClause,
    pub withClause: *mut WithClause,
    pub override_: OverridingKind,
}

/// `DeleteStmt` - a `DELETE` statement.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DeleteStmt {
    pub type_: NodeTag,
    pub relation: *mut RangeVar,
    pub usingClause: *mut List,
    pub whereClause: *mut Node,
    pub returningClause: *mut ReturningClause,
    pub withClause: *mut WithClause,
}

/// `UpdateStmt` - an `UPDATE` statement.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct UpdateStmt {
    pub type_: NodeTag,
    pub relation: *mut RangeVar,
    pub targetList: *mut List,
    pub whereClause: *mut Node,
    pub fromClause: *mut List,
    pub returningClause: *mut ReturningClause,
    pub withClause: *mut WithClause,
}

/// `MergeStmt` - a `MERGE` statement.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MergeStmt {
    pub type_: NodeTag,
    pub relation: *mut RangeVar,
    pub sourceRelation: *mut Node,
    pub joinCondition: *mut Node,
    pub mergeWhenClauses: *mut List,
    pub returningClause: *mut ReturningClause,
    pub withClause: *mut WithClause,
}

/// `SelectStmt` - a `SELECT` statement (also the leaf/internal nodes of a
/// set-operation tree).
///
/// `larg`/`rarg` self-reference [`SelectStmt`]; `intoClause`/`withClause` reuse
/// the already-modelled [`IntoClause`]/[`WithClause`] types.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SelectStmt {
    pub type_: NodeTag,
    pub distinctClause: *mut List,
    pub intoClause: *mut IntoClause,
    pub targetList: *mut List,
    pub fromClause: *mut List,
    pub whereClause: *mut Node,
    pub groupClause: *mut List,
    pub groupDistinct: bool,
    pub havingClause: *mut Node,
    pub windowClause: *mut List,
    pub valuesLists: *mut List,
    pub sortClause: *mut List,
    pub limitOffset: *mut Node,
    pub limitCount: *mut Node,
    pub limitOption: LimitOption,
    pub lockingClause: *mut List,
    pub withClause: *mut WithClause,
    pub op: SetOperation,
    pub all: bool,
    pub larg: *mut SelectStmt,
    pub rarg: *mut SelectStmt,
}

/// `SetOperationStmt` - the set-operation node of a post-analysis query tree.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SetOperationStmt {
    pub type_: NodeTag,
    pub op: SetOperation,
    pub all: bool,
    pub larg: *mut Node,
    pub rarg: *mut Node,
    pub colTypes: *mut List,
    pub colTypmods: *mut List,
    pub colCollations: *mut List,
    pub groupClauses: *mut List,
}

/// `ReturnStmt` - `RETURN` statement (inside a SQL function body).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ReturnStmt {
    pub type_: NodeTag,
    pub returnval: *mut Node,
}

/// `PLAssignStmt` - PL/pgSQL assignment statement. Like `SelectStmt`, this is
/// transformed into a `SELECT` `Query`; the target list looks more like an
/// `UPDATE`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PLAssignStmt {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub indirection: *mut List,
    pub nnames: c_int,
    pub val: *mut SelectStmt,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// Compile-time layout asserts for a few representatives.
// ---------------------------------------------------------------------------

const _: () = {
    use core::mem::{align_of, offset_of, size_of};

    // Every node struct begins with its NodeTag header at offset 0.
    assert!(offset_of!(RawStmt, type_) == 0);
    assert!(offset_of!(SelectStmt, type_) == 0);
    assert!(offset_of!(A_Const, type_) == 0);
    assert!(offset_of!(A_Expr, type_) == 0);
    assert!(offset_of!(ResTarget, type_) == 0);

    // RawStmt: NodeTag(4) + pad(4) + Node*(8) + ParseLoc(4) + ParseLoc(4) = 24.
    assert!(offset_of!(RawStmt, stmt) == 8);
    assert!(offset_of!(RawStmt, stmt_location) == 16);
    assert!(offset_of!(RawStmt, stmt_len) == 20);
    assert!(size_of::<RawStmt>() == 24);

    // A_Const: NodeTag(4) + pad(4) + ValUnion(16) + isnull(1) + pad(3)
    //          + ParseLoc(4) = 32. The inline ValUnion is pointer-aligned.
    assert!(align_of::<ValUnion>() == 8);
    assert!(size_of::<ValUnion>() == 16);
    assert!(offset_of!(A_Const, val) == 8);
    assert!(offset_of!(A_Const, isnull) == 24);
    assert!(offset_of!(A_Const, location) == 28);
    assert!(size_of::<A_Const>() == 32);

    // A_Expr: NodeTag(4) + kind(4) + name(8) + lexpr(8) + rexpr(8)
    //         + 3*ParseLoc(12) -> 44, padded to 48 (8-byte alignment).
    assert!(offset_of!(A_Expr, kind) == 4);
    assert!(offset_of!(A_Expr, name) == 8);
    assert!(offset_of!(A_Expr, lexpr) == 16);
    assert!(offset_of!(A_Expr, rexpr) == 24);
    assert!(offset_of!(A_Expr, rexpr_list_start) == 32);
    assert!(offset_of!(A_Expr, rexpr_list_end) == 36);
    assert!(offset_of!(A_Expr, location) == 40);
    assert!(size_of::<A_Expr>() == 48);

    // SelectStmt ends with two self-referential SelectStmt* children.
    assert!(
        offset_of!(SelectStmt, larg) + size_of::<*mut SelectStmt>() == offset_of!(SelectStmt, rarg)
    );

    // ReturnStmt: NodeTag(4) + pad(4) + Node*(8) -> 16.
    assert!(offset_of!(ReturnStmt, type_) == 0);
    assert!(offset_of!(ReturnStmt, returnval) == 8);
    assert!(size_of::<ReturnStmt>() == 16);

    // PLAssignStmt: NodeTag(4)+pad(4)+char*(8)+List*(8)+int(4)+pad(4)
    //               +SelectStmt*(8)+ParseLoc(4) -> 44, padded to 48.
    assert!(offset_of!(PLAssignStmt, type_) == 0);
    assert!(offset_of!(PLAssignStmt, name) == 8);
    assert!(offset_of!(PLAssignStmt, indirection) == 16);
    assert!(offset_of!(PLAssignStmt, nnames) == 24);
    assert!(offset_of!(PLAssignStmt, val) == 32);
    assert!(offset_of!(PLAssignStmt, location) == 40);
    assert!(size_of::<PLAssignStmt>() == 48);
};

// ---------------------------------------------------------------------------
// Coverage registration.
// ---------------------------------------------------------------------------

use crate::{NodeTypeCoverage, NodeTypeStatus};

/// Node types modelled by the raw-statement (DML) family.
///
/// `lib.rs` concatenates this slice with the other families' coverage into the
/// crate-wide view, so adding entries here is sufficient to register them
/// crate-wide.
pub fn node_types_covered() -> &'static [NodeTypeStatus] {
    use crate::node_tags::*;
    &[
        // Top-level optimizable / raw statements.
        NodeTypeStatus {
            name: "RawStmt",
            tag: T_RawStmt,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "InsertStmt",
            tag: T_InsertStmt,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "DeleteStmt",
            tag: T_DeleteStmt,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "UpdateStmt",
            tag: T_UpdateStmt,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "MergeStmt",
            tag: T_MergeStmt,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "SelectStmt",
            tag: T_SelectStmt,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "SetOperationStmt",
            tag: T_SetOperationStmt,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "ReturnStmt",
            tag: T_ReturnStmt,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "PLAssignStmt",
            tag: T_PLAssignStmt,
            coverage: NodeTypeCoverage::Modelled,
        },
        // Raw-grammar primitive / reference nodes.
        NodeTypeStatus {
            name: "TypeName",
            tag: T_TypeName,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "ColumnRef",
            tag: T_ColumnRef,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "ParamRef",
            tag: T_ParamRef,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "A_Expr",
            tag: T_A_Expr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "A_Const",
            tag: T_A_Const,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "TypeCast",
            tag: T_TypeCast,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "CollateClause",
            tag: T_CollateClause,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "FuncCall",
            tag: T_FuncCall,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "A_Star",
            tag: T_A_Star,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "A_Indices",
            tag: T_A_Indices,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "A_Indirection",
            tag: T_A_Indirection,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "A_ArrayExpr",
            tag: T_A_ArrayExpr,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "ResTarget",
            tag: T_ResTarget,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "MultiAssignRef",
            tag: T_MultiAssignRef,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "SortBy",
            tag: T_SortBy,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "WindowDef",
            tag: T_WindowDef,
            coverage: NodeTypeCoverage::Modelled,
        },
        // Raw-grammar FROM-clause / range nodes.
        NodeTypeStatus {
            name: "RangeSubselect",
            tag: T_RangeSubselect,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "RangeFunction",
            tag: T_RangeFunction,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "RangeTableFunc",
            tag: T_RangeTableFunc,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "RangeTableFuncCol",
            tag: T_RangeTableFuncCol,
            coverage: NodeTypeCoverage::Modelled,
        },
        NodeTypeStatus {
            name: "RangeTableSample",
            tag: T_RangeTableSample,
            coverage: NodeTypeCoverage::Modelled,
        },
        // GroupingSet / InferClause / OnConflictClause / MergeWhenClause /
        // ReturningOption / ReturningClause / CTESearchClause / CTECycleClause
        // are registered by `parsenodes::node_types_covered()` (their canonical
        // home), so they are intentionally NOT re-listed here to avoid
        // double-counting them in the crate-wide coverage table.
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{offset_of, size_of};

    #[test]
    fn raw_stmt_layout() {
        assert_eq!(offset_of!(RawStmt, type_), 0);
        assert_eq!(offset_of!(RawStmt, stmt), 8);
        assert_eq!(offset_of!(RawStmt, stmt_location), 16);
        assert_eq!(offset_of!(RawStmt, stmt_len), 20);
        assert_eq!(size_of::<RawStmt>(), 24);
    }

    #[test]
    fn a_const_inline_valunion_layout() {
        assert_eq!(size_of::<ValUnion>(), 16);
        assert_eq!(offset_of!(A_Const, val), 8);
        assert_eq!(offset_of!(A_Const, isnull), 24);
        assert_eq!(offset_of!(A_Const, location), 28);
        assert_eq!(size_of::<A_Const>(), 32);
    }

    #[test]
    fn insert_stmt_tail_override_offset() {
        // NodeTag(4)+pad(4) + 6 ptrs(48) + override(4) ... padded to 8.
        assert_eq!(offset_of!(InsertStmt, type_), 0);
        assert_eq!(offset_of!(InsertStmt, relation), 8);
        assert_eq!(offset_of!(InsertStmt, override_), 56);
        assert_eq!(size_of::<InsertStmt>(), 64);
    }

    #[test]
    fn select_stmt_self_references_are_adjacent() {
        assert_eq!(offset_of!(SelectStmt, type_), 0);
        assert_eq!(
            offset_of!(SelectStmt, larg) + size_of::<*mut SelectStmt>(),
            offset_of!(SelectStmt, rarg)
        );
    }

    #[test]
    fn every_node_has_tag_header_at_zero() {
        assert_eq!(offset_of!(TypeName, type_), 0);
        assert_eq!(offset_of!(ColumnRef, type_), 0);
        assert_eq!(offset_of!(A_Expr, type_), 0);
        assert_eq!(offset_of!(FuncCall, type_), 0);
        assert_eq!(offset_of!(SortBy, type_), 0);
        assert_eq!(offset_of!(SetOperationStmt, type_), 0);
        assert_eq!(offset_of!(CTECycleClause, type_), 0);
    }
}
