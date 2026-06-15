use core::ffi::{c_char, c_int, c_void};

use crate::{AttrNumber, Index, List, Node, NodeTag, Oid};

pub type ParseLoc = c_int;

/// `ParseExprKind` (`parser/parse_node.h`) — kind of expression being parsed.
pub type ParseExprKind = c_uint;
use core::ffi::c_uint;

pub const EXPR_KIND_NONE: ParseExprKind = 0;
pub const EXPR_KIND_OTHER: ParseExprKind = 1;
pub const EXPR_KIND_JOIN_ON: ParseExprKind = 2;
pub const EXPR_KIND_JOIN_USING: ParseExprKind = 3;
pub const EXPR_KIND_FROM_SUBSELECT: ParseExprKind = 4;
pub const EXPR_KIND_FROM_FUNCTION: ParseExprKind = 5;
pub const EXPR_KIND_WHERE: ParseExprKind = 6;
pub const EXPR_KIND_HAVING: ParseExprKind = 7;
pub const EXPR_KIND_FILTER: ParseExprKind = 8;
pub const EXPR_KIND_WINDOW_PARTITION: ParseExprKind = 9;
pub const EXPR_KIND_WINDOW_ORDER: ParseExprKind = 10;
pub const EXPR_KIND_WINDOW_FRAME_RANGE: ParseExprKind = 11;
pub const EXPR_KIND_WINDOW_FRAME_ROWS: ParseExprKind = 12;
pub const EXPR_KIND_WINDOW_FRAME_GROUPS: ParseExprKind = 13;
pub const EXPR_KIND_SELECT_TARGET: ParseExprKind = 14;
pub const EXPR_KIND_INSERT_TARGET: ParseExprKind = 15;
pub const EXPR_KIND_UPDATE_SOURCE: ParseExprKind = 16;
pub const EXPR_KIND_UPDATE_TARGET: ParseExprKind = 17;
pub const EXPR_KIND_MERGE_WHEN: ParseExprKind = 18;
pub const EXPR_KIND_GROUP_BY: ParseExprKind = 19;
pub const EXPR_KIND_ORDER_BY: ParseExprKind = 20;
pub const EXPR_KIND_DISTINCT_ON: ParseExprKind = 21;
pub const EXPR_KIND_LIMIT: ParseExprKind = 22;
pub const EXPR_KIND_OFFSET: ParseExprKind = 23;
pub const EXPR_KIND_RETURNING: ParseExprKind = 24;
pub const EXPR_KIND_MERGE_RETURNING: ParseExprKind = 25;
pub const EXPR_KIND_VALUES: ParseExprKind = 26;
pub const EXPR_KIND_VALUES_SINGLE: ParseExprKind = 27;
pub const EXPR_KIND_CHECK_CONSTRAINT: ParseExprKind = 28;
pub const EXPR_KIND_DOMAIN_CHECK: ParseExprKind = 29;
pub const EXPR_KIND_COLUMN_DEFAULT: ParseExprKind = 30;
pub const EXPR_KIND_FUNCTION_DEFAULT: ParseExprKind = 31;
pub const EXPR_KIND_INDEX_EXPRESSION: ParseExprKind = 32;
pub const EXPR_KIND_INDEX_PREDICATE: ParseExprKind = 33;
pub const EXPR_KIND_STATS_EXPRESSION: ParseExprKind = 34;
pub const EXPR_KIND_ALTER_COL_TRANSFORM: ParseExprKind = 35;
pub const EXPR_KIND_EXECUTE_PARAMETER: ParseExprKind = 36;
pub const EXPR_KIND_TRIGGER_WHEN: ParseExprKind = 37;
pub const EXPR_KIND_POLICY: ParseExprKind = 38;
pub const EXPR_KIND_PARTITION_BOUND: ParseExprKind = 39;
pub const EXPR_KIND_PARTITION_EXPRESSION: ParseExprKind = 40;
pub const EXPR_KIND_CALL_ARGUMENT: ParseExprKind = 41;
pub const EXPR_KIND_COPY_WHERE: ParseExprKind = 42;
pub const EXPR_KIND_GENERATED_COLUMN: ParseExprKind = 43;
pub const EXPR_KIND_CYCLE_MARK: ParseExprKind = 44;

/// `VarReturningType` mirror (`primnodes.h`) — re-exported for parser structs.
pub type VarReturningType = c_uint;

/// `ParseState` — parser working state (`parser/parse_node.h`).
///
/// Exact-ABI mirror of the C `struct ParseState`. Genuinely-external pointee
/// types (`QueryEnvironment`, `Relation`, the parser hook function pointers,
/// passthrough hook state) are spelled as opaque pointers, which are
/// layout-identical to the C members they stand in for (one pointer each). The
/// `ParseNamespaceItem *` members are typed against the modelled struct below.
#[repr(C)]
pub struct ParseState {
    pub parentParseState: *mut ParseState,
    pub p_sourcetext: *const c_char,
    pub p_rtable: *mut List,
    pub p_rteperminfos: *mut List,
    pub p_joinexprs: *mut List,
    pub p_nullingrels: *mut List,
    pub p_joinlist: *mut List,
    pub p_namespace: *mut List,
    pub p_lateral_active: bool,
    pub p_ctenamespace: *mut List,
    pub p_future_ctes: *mut List,
    pub p_parent_cte: *mut Node,
    pub p_target_relation: *mut c_void,
    pub p_target_nsitem: *mut ParseNamespaceItem,
    pub p_grouping_nsitem: *mut ParseNamespaceItem,
    pub p_is_insert: bool,
    pub p_windowdefs: *mut List,
    pub p_expr_kind: ParseExprKind,
    pub p_next_resno: c_int,
    pub p_multiassign_exprs: *mut List,
    pub p_locking_clause: *mut List,
    pub p_locked_from_parent: bool,
    pub p_resolve_unknowns: bool,
    pub p_queryEnv: *mut c_void,
    pub p_hasAggs: bool,
    pub p_hasWindowFuncs: bool,
    pub p_hasTargetSRFs: bool,
    pub p_hasSubLinks: bool,
    pub p_hasModifyingCTE: bool,
    pub p_last_srf: *mut Node,
    pub p_pre_columnref_hook: *mut c_void,
    pub p_post_columnref_hook: *mut c_void,
    pub p_paramref_hook: *mut c_void,
    pub p_coerce_param_hook: *mut c_void,
    pub p_ref_hook_state: *mut c_void,
}

/// `ParseNamespaceItem` — an element of a namespace list (`parse_node.h`).
///
/// `p_names`/`p_rte`/`p_perminfo` point at the modelled `Alias`,
/// `RangeTblEntry`, and `RTEPermissionInfo` structs (kept opaque here to avoid a
/// dependency cycle; the parser crate casts them to the modelled types).
#[repr(C)]
pub struct ParseNamespaceItem {
    pub p_names: *mut c_void,
    pub p_rte: *mut c_void,
    pub p_rtindex: c_int,
    pub p_perminfo: *mut c_void,
    pub p_nscolumns: *mut ParseNamespaceColumn,
    pub p_rel_visible: bool,
    pub p_cols_visible: bool,
    pub p_lateral_only: bool,
    pub p_lateral_ok: bool,
    pub p_returning_type: VarReturningType,
}

/// `ParseNamespaceColumn` — data about one column of a `ParseNamespaceItem`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ParseNamespaceColumn {
    pub p_varno: Index,
    pub p_varattno: AttrNumber,
    pub p_vartype: Oid,
    pub p_vartypmod: i32,
    pub p_varcollid: Oid,
    pub p_varreturningtype: VarReturningType,
    pub p_varnosyn: Index,
    pub p_varattnosyn: AttrNumber,
    pub p_dontexpand: bool,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct TypeName {
    pub type_: NodeTag,
    pub names: *mut List,
    pub typeOid: Oid,
    pub setof: bool,
    pub pct_type: bool,
    pub typmods: *mut List,
    pub typemod: c_int,
    pub arrayBounds: *mut List,
    pub location: ParseLoc,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn parse_node_struct_layouts_match_postgres() {
        // ParseNamespaceColumn: see parse_node.h struct ParseNamespaceColumn.
        // AttrNumber is int16, so p_varattno/p_varattnosyn occupy 2 bytes.
        assert_eq!(size_of::<ParseNamespaceColumn>(), 32);
        assert_eq!(align_of::<ParseNamespaceColumn>(), 4);
        assert_eq!(offset_of!(ParseNamespaceColumn, p_varno), 0);
        assert_eq!(offset_of!(ParseNamespaceColumn, p_varattno), 4);
        assert_eq!(offset_of!(ParseNamespaceColumn, p_vartype), 8);
        assert_eq!(offset_of!(ParseNamespaceColumn, p_vartypmod), 12);
        assert_eq!(offset_of!(ParseNamespaceColumn, p_varcollid), 16);
        assert_eq!(offset_of!(ParseNamespaceColumn, p_varreturningtype), 20);
        assert_eq!(offset_of!(ParseNamespaceColumn, p_varnosyn), 24);
        assert_eq!(offset_of!(ParseNamespaceColumn, p_varattnosyn), 28);
        assert_eq!(offset_of!(ParseNamespaceColumn, p_dontexpand), 30);

        // ParseNamespaceItem: see parse_node.h struct ParseNamespaceItem.
        assert_eq!(align_of::<ParseNamespaceItem>(), 8);
        assert_eq!(offset_of!(ParseNamespaceItem, p_names), 0);
        assert_eq!(offset_of!(ParseNamespaceItem, p_rte), 8);
        assert_eq!(offset_of!(ParseNamespaceItem, p_rtindex), 16);
        assert_eq!(offset_of!(ParseNamespaceItem, p_perminfo), 24);
        assert_eq!(offset_of!(ParseNamespaceItem, p_nscolumns), 32);
        assert_eq!(offset_of!(ParseNamespaceItem, p_rel_visible), 40);
        assert_eq!(offset_of!(ParseNamespaceItem, p_cols_visible), 41);
        assert_eq!(offset_of!(ParseNamespaceItem, p_lateral_only), 42);
        assert_eq!(offset_of!(ParseNamespaceItem, p_lateral_ok), 43);
        assert_eq!(offset_of!(ParseNamespaceItem, p_returning_type), 44);
        assert_eq!(size_of::<ParseNamespaceItem>(), 48);

        // ParseState: see parse_node.h struct ParseState.
        assert_eq!(align_of::<ParseState>(), 8);
        assert_eq!(offset_of!(ParseState, parentParseState), 0);
        assert_eq!(offset_of!(ParseState, p_sourcetext), 8);
        assert_eq!(offset_of!(ParseState, p_rtable), 16);
        assert_eq!(offset_of!(ParseState, p_namespace), 56);
        assert_eq!(offset_of!(ParseState, p_lateral_active), 64);
        assert_eq!(offset_of!(ParseState, p_target_nsitem), 104);
        assert_eq!(offset_of!(ParseState, p_grouping_nsitem), 112);
        assert_eq!(offset_of!(ParseState, p_windowdefs), 128);
        assert_eq!(offset_of!(ParseState, p_expr_kind), 136);
        assert_eq!(offset_of!(ParseState, p_next_resno), 140);
        assert_eq!(offset_of!(ParseState, p_locking_clause), 152);
        assert_eq!(offset_of!(ParseState, p_queryEnv), 168);
        assert_eq!(offset_of!(ParseState, p_last_srf), 184);
        assert_eq!(offset_of!(ParseState, p_ref_hook_state), 224);
        assert_eq!(size_of::<ParseState>(), 232);
    }

    #[test]
    fn typename_layout_matches_postgres() {
        assert_eq!(size_of::<TypeName>(), 56);
        assert_eq!(align_of::<TypeName>(), 8);
        assert_eq!(offset_of!(TypeName, type_), 0);
        assert_eq!(offset_of!(TypeName, names), 8);
        assert_eq!(offset_of!(TypeName, typeOid), 16);
        assert_eq!(offset_of!(TypeName, setof), 20);
        assert_eq!(offset_of!(TypeName, pct_type), 21);
        assert_eq!(offset_of!(TypeName, typmods), 24);
        assert_eq!(offset_of!(TypeName, typemod), 32);
        assert_eq!(offset_of!(TypeName, arrayBounds), 40);
        assert_eq!(offset_of!(TypeName, location), 48);
    }
}
