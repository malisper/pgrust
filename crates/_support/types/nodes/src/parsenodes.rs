// Analyzed query-tree nodes; field names/order mirror vendor/parsenodes.h
// (tests: *_field_order_matches_c, enum_values_match_c_headers).
#![allow(non_camel_case_types, non_snake_case)]

use types_core::{Cardinality, Index, Oid, ParseLoc};

use crate::bitmapset::Bitmapset;
use crate::jointype::JoinType;
use crate::list::{IntList, NodeList, OidList};
use crate::node_tree::{Node, NodeVariant};
use crate::nodes_enums::{CmdType, LimitOption};
use crate::primnodes::{Alias, FromExpr, OverridingKind};
use crate::tags::NodeTag;

pub type AclMode = u64;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum QuerySource {
    #[default]
    QSRC_ORIGINAL = 0,
    QSRC_PARSER = 1,
    QSRC_INSTEAD_RULE = 2,
    QSRC_QUAL_INSTEAD_RULE = 3,
    QSRC_NON_INSTEAD_RULE = 4,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum SetOperation {
    #[default]
    SETOP_NONE = 0,
    SETOP_UNION = 1,
    SETOP_INTERSECT = 2,
    SETOP_EXCEPT = 3,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum RTEKind {
    #[default]
    RTE_RELATION = 0,
    RTE_SUBQUERY = 1,
    RTE_JOIN = 2,
    RTE_FUNCTION = 3,
    RTE_TABLEFUNC = 4,
    RTE_VALUES = 5,
    RTE_CTE = 6,
    RTE_NAMEDTUPLESTORE = 7,
    RTE_RESULT = 8,
    RTE_GROUP = 9,
}

#[derive(Default)]
pub struct Query<'mcx> {
    pub commandType: CmdType,
    pub querySource: QuerySource,
    pub queryId: i64,
    pub canSetTag: bool,
    pub utilityStmt: Option<Node<'mcx>>,
    pub resultRelation: i32,
    pub hasAggs: bool,
    pub hasWindowFuncs: bool,
    pub hasTargetSRFs: bool,
    pub hasSubLinks: bool,
    pub hasDistinctOn: bool,
    pub hasRecursive: bool,
    pub hasModifyingCTE: bool,
    pub hasForUpdate: bool,
    pub hasRowSecurity: bool,
    pub hasGroupRTE: bool,
    pub isReturn: bool,
    pub cteList: NodeList<'mcx>,
    pub rtable: NodeList<'mcx>,
    pub rteperminfos: NodeList<'mcx>,
    pub jointree: Option<&'mcx FromExpr<'mcx>>,
    pub mergeActionList: NodeList<'mcx>,
    pub mergeTargetRelation: i32,
    pub mergeJoinCondition: Option<Node<'mcx>>,
    pub targetList: NodeList<'mcx>,
    pub r#override: OverridingKind,
    pub onConflict: Option<Node<'mcx>>,
    pub returningOldAlias: Option<&'mcx str>,
    pub returningNewAlias: Option<&'mcx str>,
    pub returningList: NodeList<'mcx>,
    pub groupClause: NodeList<'mcx>,
    pub groupDistinct: bool,
    pub groupingSets: NodeList<'mcx>,
    pub havingQual: Option<Node<'mcx>>,
    pub windowClause: NodeList<'mcx>,
    pub distinctClause: NodeList<'mcx>,
    pub sortClause: NodeList<'mcx>,
    pub limitOffset: Option<Node<'mcx>>,
    pub limitCount: Option<Node<'mcx>>,
    pub limitOption: LimitOption,
    pub rowMarks: NodeList<'mcx>,
    pub setOperations: Option<Node<'mcx>>,
    pub constraintDeps: OidList<'mcx>,
    pub withCheckOptions: NodeList<'mcx>,
    pub stmt_location: ParseLoc,
    pub stmt_len: ParseLoc,
}

#[derive(Default)]
pub struct RangeTblEntry<'mcx> {
    pub alias: Option<&'mcx Alias<'mcx>>,
    pub eref: Option<&'mcx Alias<'mcx>>,
    pub rtekind: RTEKind,
    pub relid: Oid,
    pub inh: bool,
    pub relkind: u8,
    pub rellockmode: i32,
    pub perminfoindex: Index,
    pub tablesample: Option<Node<'mcx>>,
    pub subquery: Option<&'mcx Query<'mcx>>,
    pub security_barrier: bool,
    pub jointype: JoinType,
    pub joinmergedcols: i32,
    pub joinaliasvars: NodeList<'mcx>,
    pub joinleftcols: IntList<'mcx>,
    pub joinrightcols: IntList<'mcx>,
    pub join_using_alias: Option<&'mcx Alias<'mcx>>,
    pub functions: NodeList<'mcx>,
    pub funcordinality: bool,
    pub tablefunc: Option<Node<'mcx>>,
    pub values_lists: NodeList<'mcx>,
    pub ctename: Option<&'mcx str>,
    pub ctelevelsup: Index,
    pub self_reference: bool,
    pub coltypes: OidList<'mcx>,
    pub coltypmods: IntList<'mcx>,
    pub colcollations: OidList<'mcx>,
    pub enrname: Option<&'mcx str>,
    pub enrtuples: Cardinality,
    pub groupexprs: NodeList<'mcx>,
    pub lateral: bool,
    pub inFromCl: bool,
    pub securityQuals: NodeList<'mcx>,
}

pub struct RTEPermissionInfo<'mcx> {
    pub relid: Oid,
    pub inh: bool,
    pub requiredPerms: AclMode,
    pub checkAsUser: Oid,
    pub selectedCols: Bitmapset<'mcx>,
    pub insertedCols: Bitmapset<'mcx>,
    pub updatedCols: Bitmapset<'mcx>,
}

impl Default for RTEPermissionInfo<'_> {
    fn default() -> Self {
        RTEPermissionInfo {
            relid: 0,
            inh: false,
            requiredPerms: 0,
            checkAsUser: 0,
            selectedCols: Bitmapset::empty(),
            insertedCols: Bitmapset::empty(),
            updatedCols: Bitmapset::empty(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum TransactionStmtKind {
    #[default]
    TRANS_STMT_BEGIN = 0,
    TRANS_STMT_START = 1,
    TRANS_STMT_COMMIT = 2,
    TRANS_STMT_ROLLBACK = 3,
    TRANS_STMT_SAVEPOINT = 4,
    TRANS_STMT_RELEASE = 5,
    TRANS_STMT_ROLLBACK_TO = 6,
    TRANS_STMT_PREPARE = 7,
    TRANS_STMT_COMMIT_PREPARED = 8,
    TRANS_STMT_ROLLBACK_PREPARED = 9,
}

pub struct TransactionStmt<'mcx> {
    pub kind: TransactionStmtKind,
    pub options: NodeList<'mcx>,
    pub savepoint_name: Option<&'mcx str>,
    pub gid: Option<&'mcx str>,
    pub chain: bool,
    pub location: ParseLoc,
}

impl Default for TransactionStmt<'_> {
    fn default() -> Self {
        TransactionStmt {
            kind: TransactionStmtKind::TRANS_STMT_BEGIN,
            options: NodeList::nil(),
            savepoint_name: None,
            gid: None,
            chain: false,
            location: -1,
        }
    }
}

// SAFETY (each): tag/type pairing mirrors parsenodes.h.
unsafe impl<'mcx> NodeVariant<'mcx> for Query<'mcx> {
    const TAG: NodeTag = NodeTag::T_Query;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RangeTblEntry<'mcx> {
    const TAG: NodeTag = NodeTag::T_RangeTblEntry;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RTEPermissionInfo<'mcx> {
    const TAG: NodeTag = NodeTag::T_RTEPermissionInfo;
}
unsafe impl<'mcx> NodeVariant<'mcx> for TransactionStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_TransactionStmt;
}

impl<'mcx> Node<'mcx> {
    #[inline]
    pub fn as_query(self) -> Option<&'mcx Query<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_range_tbl_entry(self) -> Option<&'mcx RangeTblEntry<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_rte_permission_info(self) -> Option<&'mcx RTEPermissionInfo<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_transaction_stmt(self) -> Option<&'mcx TransactionStmt<'mcx>> {
        self.as_variant()
    }
}
