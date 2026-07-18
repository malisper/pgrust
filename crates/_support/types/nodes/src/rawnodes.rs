// Raw-grammar input nodes; field names/order mirror vendor/parsenodes.h
// (tests: *_field_order_matches_c, enum_values_match_c_headers).
#![allow(non_camel_case_types, non_snake_case)]

use mcx::Mcx;
use types_core::{Oid, ParseLoc};
use types_error::PgResult;

use crate::list::{NodeList, OidList};
use crate::node_tree::{BitString, Boolean, Float, Integer, Node, NodeVariant, String};
use crate::nodes_enums::{LimitOption, LockClauseStrength, LockWaitPolicy};
use crate::parsenodes::SetOperation;
use crate::tags::NodeTag;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum A_Expr_Kind {
    #[default]
    AEXPR_OP = 0,
    AEXPR_OP_ANY = 1,
    AEXPR_OP_ALL = 2,
    AEXPR_DISTINCT = 3,
    AEXPR_NOT_DISTINCT = 4,
    AEXPR_NULLIF = 5,
    AEXPR_IN = 6,
    AEXPR_LIKE = 7,
    AEXPR_ILIKE = 8,
    AEXPR_SIMILAR = 9,
    AEXPR_BETWEEN = 10,
    AEXPR_NOT_BETWEEN = 11,
    AEXPR_BETWEEN_SYM = 12,
    AEXPR_NOT_BETWEEN_SYM = 13,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum SortByDir {
    #[default]
    SORTBY_DEFAULT = 0,
    SORTBY_ASC = 1,
    SORTBY_DESC = 2,
    SORTBY_USING = 3,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum SortByNulls {
    #[default]
    SORTBY_NULLS_DEFAULT = 0,
    SORTBY_NULLS_FIRST = 1,
    SORTBY_NULLS_LAST = 2,
}

// C divergence: C's SelectStmt.distinctClause encodes plain DISTINCT as
// list_make1(NIL) — a one-NULL-cell list — and DISTINCT ON as the expression
// list. NodeList cells are non-null, so the three states are explicit here.
#[derive(Default)]
pub enum DistinctClause<'mcx> {
    #[default]
    None,
    All,
    On(NodeList<'mcx>),
}

impl DistinctClause<'_> {
    #[inline]
    pub fn is_none(&self) -> bool {
        matches!(self, DistinctClause::None)
    }
}

#[derive(Clone, Copy, Default)]
pub struct RawStmt<'mcx> {
    pub stmt: Option<Node<'mcx>>,
    pub stmt_location: ParseLoc,
    pub stmt_len: ParseLoc,
}

#[derive(Default)]
pub struct SelectStmt<'mcx> {
    pub distinctClause: DistinctClause<'mcx>,
    pub intoClause: Option<Node<'mcx>>,
    pub targetList: NodeList<'mcx>,
    pub fromClause: NodeList<'mcx>,
    pub whereClause: Option<Node<'mcx>>,
    pub groupClause: NodeList<'mcx>,
    pub groupDistinct: bool,
    pub havingClause: Option<Node<'mcx>>,
    pub windowClause: NodeList<'mcx>,
    pub valuesLists: NodeList<'mcx>,
    pub sortClause: NodeList<'mcx>,
    pub limitOffset: Option<Node<'mcx>>,
    pub limitCount: Option<Node<'mcx>>,
    pub limitOption: LimitOption,
    pub lockingClause: NodeList<'mcx>,
    pub withClause: Option<Node<'mcx>>,
    pub op: SetOperation,
    pub all: bool,
    pub larg: Option<&'mcx SelectStmt<'mcx>>,
    pub rarg: Option<&'mcx SelectStmt<'mcx>>,
}

#[derive(Default)]
pub struct LockingClause<'mcx> {
    pub lockedRels: NodeList<'mcx>,
    pub strength: LockClauseStrength,
    pub waitPolicy: LockWaitPolicy,
}

/// `relation` is a RangeVar node handle (the grammar scribbles its alias).
#[derive(Default)]
pub struct InsertStmt<'mcx> {
    pub relation: Option<Node<'mcx>>,
    pub cols: NodeList<'mcx>,
    pub selectStmt: Option<Node<'mcx>>,
    pub onConflictClause: Option<Node<'mcx>>,
    pub returningClause: Option<Node<'mcx>>,
    pub withClause: Option<Node<'mcx>>,
    pub r#override: crate::primnodes::OverridingKind,
}

// ReturningOptionKind (parsenodes.h).
#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
#[repr(i32)]
pub enum ReturningOptionKind {
    #[default]
    RETURNING_OPTION_OLD = 0,
    RETURNING_OPTION_NEW = 1,
}

#[derive(Default)]
pub struct ReturningOption<'mcx> {
    pub option: ReturningOptionKind,
    pub value: Option<&'mcx str>,
    pub location: ParseLoc,
}

/// `options` cells are ReturningOption nodes (the PG18 OLD/NEW alias lane).
#[derive(Default)]
pub struct ReturningClause<'mcx> {
    pub options: NodeList<'mcx>,
    pub exprs: NodeList<'mcx>,
}

/// `relation` is a RangeVar node; `sourceRelation` a table_ref;
/// `mergeWhenClauses` cells are MergeWhenClause.
#[derive(Default)]
pub struct MergeStmt<'mcx> {
    pub relation: Option<Node<'mcx>>,
    pub sourceRelation: Option<Node<'mcx>>,
    pub joinCondition: Option<Node<'mcx>>,
    pub mergeWhenClauses: NodeList<'mcx>,
    pub returningClause: Option<Node<'mcx>>,
    pub withClause: Option<Node<'mcx>>,
}

#[derive(Default)]
pub struct MergeWhenClause<'mcx> {
    pub matchKind: crate::primnodes::MergeMatchKind,
    pub commandType: crate::nodes_enums::CmdType,
    pub r#override: crate::primnodes::OverridingKind,
    pub condition: Option<Node<'mcx>>,
    pub targetList: NodeList<'mcx>,
    pub values: NodeList<'mcx>,
}

/// `infer` is an InferClause node; `targetList` cells are ResTarget.
#[derive(Default)]
pub struct OnConflictClause<'mcx> {
    pub action: crate::primnodes::OnConflictAction,
    pub infer: Option<Node<'mcx>>,
    pub targetList: NodeList<'mcx>,
    pub whereClause: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

/// `indexElems` cells are IndexElem.
#[derive(Default)]
pub struct InferClause<'mcx> {
    pub indexElems: NodeList<'mcx>,
    pub whereClause: Option<Node<'mcx>>,
    pub conname: Option<&'mcx str>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct DeleteStmt<'mcx> {
    pub relation: Option<Node<'mcx>>,
    pub usingClause: NodeList<'mcx>,
    pub whereClause: Option<Node<'mcx>>,
    pub returningClause: Option<Node<'mcx>>,
    pub withClause: Option<Node<'mcx>>,
}

#[derive(Default)]
pub struct UpdateStmt<'mcx> {
    pub relation: Option<Node<'mcx>>,
    pub targetList: NodeList<'mcx>,
    pub whereClause: Option<Node<'mcx>>,
    pub fromClause: NodeList<'mcx>,
    pub returningClause: Option<Node<'mcx>>,
    pub withClause: Option<Node<'mcx>>,
}

#[derive(Default)]
pub struct ResTarget<'mcx> {
    pub name: Option<&'mcx str>,
    pub indirection: NodeList<'mcx>,
    pub val: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct CompositeTypeStmt<'mcx> {
    pub typevar: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub coldeflist: NodeList<'mcx>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum AlterTSConfigType {
    #[default]
    ALTER_TSCONFIG_ADD_MAPPING = 0,
    ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN = 1,
    ALTER_TSCONFIG_REPLACE_DICT = 2,
    ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN = 3,
    ALTER_TSCONFIG_DROP_MAPPING = 4,
}

#[derive(Default)]
pub struct AlterTSDictionaryStmt<'mcx> {
    pub dictname: NodeList<'mcx>,
    pub options: NodeList<'mcx>,
}

#[derive(Default)]
pub struct AlterTSConfigurationStmt<'mcx> {
    pub kind: AlterTSConfigType,
    pub cfgname: NodeList<'mcx>,
    pub tokentype: NodeList<'mcx>,
    pub dicts: NodeList<'mcx>,
    pub r#override: bool,
    pub replace: bool,
    pub missing_ok: bool,
}

#[derive(Default)]
pub struct A_Expr<'mcx> {
    pub kind: A_Expr_Kind,
    pub name: NodeList<'mcx>,
    pub lexpr: Option<Node<'mcx>>,
    pub rexpr: Option<Node<'mcx>>,
    pub rexpr_list_start: ParseLoc,
    pub rexpr_list_end: ParseLoc,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct CollateClause<'mcx> {
    pub arg: Option<Node<'mcx>>,
    pub collname: NodeList<'mcx>,
    pub location: ParseLoc,
}

/// C `union ValUnion` — the embedded value-node union of `A_Const`.
#[derive(Clone, Copy)]
pub enum ValUnion<'mcx> {
    Integer(Integer),
    Float(Float<'mcx>),
    Boolean(Boolean),
    String(String<'mcx>),
    BitString(BitString<'mcx>),
}

// C divergence: C pairs an undefined-when-isnull union with a separate
// `bool isnull`; `val: None` IS the null case here (no undefined reads).
#[derive(Clone, Copy, Default)]
pub struct A_Const<'mcx> {
    pub val: Option<ValUnion<'mcx>>,
    pub location: ParseLoc,
}

impl A_Const<'_> {
    #[inline]
    pub fn isnull(&self) -> bool {
        self.val.is_none()
    }
}

#[derive(Default)]
pub struct ColumnRef<'mcx> {
    pub fields: NodeList<'mcx>,
    pub location: ParseLoc,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ParamRef {
    pub number: i32,
    pub location: ParseLoc,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct A_Star;

#[derive(Default)]
pub struct A_Indices<'mcx> {
    pub is_slice: bool,
    pub lidx: Option<Node<'mcx>>,
    pub uidx: Option<Node<'mcx>>,
}

#[derive(Default)]
pub struct A_Indirection<'mcx> {
    pub arg: Option<Node<'mcx>>,
    pub indirection: NodeList<'mcx>,
}

#[derive(Default)]
pub struct MultiAssignRef<'mcx> {
    pub source: Option<Node<'mcx>>,
    pub colno: i32,
    pub ncolumns: i32,
}

#[derive(Default)]
pub struct A_ArrayExpr<'mcx> {
    pub elements: NodeList<'mcx>,
    pub list_start: ParseLoc,
    pub list_end: ParseLoc,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct SortBy<'mcx> {
    pub node: Option<Node<'mcx>>,
    pub sortby_dir: SortByDir,
    pub sortby_nulls: SortByNulls,
    pub useOp: NodeList<'mcx>,
    pub location: ParseLoc,
}

// Values verified against parsenodes.h FRAMEOPTION_* (frameoption_values test).
pub const FRAMEOPTION_NONDEFAULT: i32 = 0x00001;
pub const FRAMEOPTION_RANGE: i32 = 0x00002;
pub const FRAMEOPTION_ROWS: i32 = 0x00004;
pub const FRAMEOPTION_GROUPS: i32 = 0x00008;
pub const FRAMEOPTION_BETWEEN: i32 = 0x00010;
pub const FRAMEOPTION_START_UNBOUNDED_PRECEDING: i32 = 0x00020;
pub const FRAMEOPTION_END_UNBOUNDED_PRECEDING: i32 = 0x00040;
pub const FRAMEOPTION_START_UNBOUNDED_FOLLOWING: i32 = 0x00080;
pub const FRAMEOPTION_END_UNBOUNDED_FOLLOWING: i32 = 0x00100;
pub const FRAMEOPTION_START_CURRENT_ROW: i32 = 0x00200;
pub const FRAMEOPTION_END_CURRENT_ROW: i32 = 0x00400;
pub const FRAMEOPTION_START_OFFSET_PRECEDING: i32 = 0x00800;
pub const FRAMEOPTION_END_OFFSET_PRECEDING: i32 = 0x01000;
pub const FRAMEOPTION_START_OFFSET_FOLLOWING: i32 = 0x02000;
pub const FRAMEOPTION_END_OFFSET_FOLLOWING: i32 = 0x04000;
pub const FRAMEOPTION_EXCLUDE_CURRENT_ROW: i32 = 0x08000;
pub const FRAMEOPTION_EXCLUDE_GROUP: i32 = 0x10000;
pub const FRAMEOPTION_EXCLUDE_TIES: i32 = 0x20000;
pub const FRAMEOPTION_START_OFFSET: i32 =
    FRAMEOPTION_START_OFFSET_PRECEDING | FRAMEOPTION_START_OFFSET_FOLLOWING;
pub const FRAMEOPTION_END_OFFSET: i32 =
    FRAMEOPTION_END_OFFSET_PRECEDING | FRAMEOPTION_END_OFFSET_FOLLOWING;
pub const FRAMEOPTION_EXCLUSION: i32 =
    FRAMEOPTION_EXCLUDE_CURRENT_ROW | FRAMEOPTION_EXCLUDE_GROUP | FRAMEOPTION_EXCLUDE_TIES;
pub const FRAMEOPTION_DEFAULTS: i32 =
    FRAMEOPTION_RANGE | FRAMEOPTION_START_UNBOUNDED_PRECEDING | FRAMEOPTION_END_CURRENT_ROW;

pub struct WindowDef<'mcx> {
    pub name: Option<&'mcx str>,
    pub refname: Option<&'mcx str>,
    pub partitionClause: NodeList<'mcx>,
    pub orderClause: NodeList<'mcx>,
    pub frameOptions: i32,
    pub startOffset: Option<Node<'mcx>>,
    pub endOffset: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

impl Default for WindowDef<'_> {
    fn default() -> Self {
        WindowDef {
            name: None,
            refname: None,
            partitionClause: NodeList::nil(),
            orderClause: NodeList::nil(),
            frameOptions: FRAMEOPTION_DEFAULTS,
            startOffset: None,
            endOffset: None,
            location: -1,
        }
    }
}

#[derive(Default)]
pub struct FuncCall<'mcx> {
    pub funcname: NodeList<'mcx>,
    pub args: NodeList<'mcx>,
    pub agg_order: NodeList<'mcx>,
    pub agg_filter: Option<Node<'mcx>>,
    pub over: Option<Node<'mcx>>,
    pub agg_within_group: bool,
    pub agg_star: bool,
    pub agg_distinct: bool,
    pub func_variadic: bool,
    pub funcformat: crate::primnodes::CoercionForm,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct RangeSubselect<'mcx> {
    pub lateral: bool,
    pub subquery: Option<Node<'mcx>>,
    pub alias: Option<&'mcx crate::primnodes::Alias<'mcx>>,
}

#[derive(Default)]
pub struct RangeTableSample<'mcx> {
    pub relation: Option<Node<'mcx>>,
    pub method: NodeList<'mcx>,
    pub args: NodeList<'mcx>,
    pub repeatable: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct RangeFunction<'mcx> {
    pub lateral: bool,
    pub ordinality: bool,
    pub is_rowsfrom: bool,
    pub functions: NodeList<'mcx>,
    pub alias: Option<&'mcx crate::primnodes::Alias<'mcx>>,
    pub coldeflist: NodeList<'mcx>,
}


#[derive(Default)]
pub struct RangeTableFunc<'mcx> {
    pub lateral: bool,
    pub docexpr: Option<Node<'mcx>>,
    pub rowexpr: Option<Node<'mcx>>,
    pub namespaces: NodeList<'mcx>,
    pub columns: NodeList<'mcx>,
    pub alias: Option<&'mcx crate::primnodes::Alias<'mcx>>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct RangeTableFuncCol<'mcx> {
    pub colname: Option<&'mcx str>,
    pub typeName: Option<Node<'mcx>>,
    pub for_ordinality: bool,
    pub is_not_null: bool,
    pub colexpr: Option<Node<'mcx>>,
    pub coldefexpr: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct XmlSerialize<'mcx> {
    pub xmloption: crate::primnodes::XmlOptionType,
    pub expr: Option<Node<'mcx>>,
    pub typeName: Option<Node<'mcx>>,
    pub indent: bool,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct TypeName<'mcx> {
    pub names: NodeList<'mcx>,
    pub typeOid: Oid,
    pub setof: bool,
    pub pct_type: bool,
    pub typmods: NodeList<'mcx>,
    pub typemod: i32,
    pub arrayBounds: NodeList<'mcx>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct TypeCast<'mcx> {
    pub arg: Option<Node<'mcx>>,
    pub typeName: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

// C OnCommitAction (primnodes.h).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OnCommitAction {
    #[default]
    ONCOMMIT_NOOP = 0,
    ONCOMMIT_PRESERVE_ROWS,
    ONCOMMIT_DELETE_ROWS,
    ONCOMMIT_DROP,
}

/// `rel` is a RangeVar node handle (the grammar scribbles its
/// relpersistence); `viewQuery` is a Query node handle (matview lane).
#[derive(Default)]
pub struct IntoClause<'mcx> {
    pub rel: Option<Node<'mcx>>,
    pub colNames: NodeList<'mcx>,
    pub accessMethod: Option<&'mcx str>,
    pub options: NodeList<'mcx>,
    pub onCommit: OnCommitAction,
    pub tableSpaceName: Option<&'mcx str>,
    pub viewQuery: Option<Node<'mcx>>,
    pub skipData: bool,
}

/// `query` is a raw statement node until parse analysis rewrites it into a
/// Query node in place; `into` is an IntoClause node handle.
#[derive(Default)]
pub struct CreateTableAsStmt<'mcx> {
    pub query: Option<Node<'mcx>>,
    pub into: Option<Node<'mcx>>,
    pub objtype: crate::parsenodes::ObjectType,
    pub is_select_into: bool,
    pub if_not_exists: bool,
}

#[derive(Default)]
pub struct RefreshMatViewStmt<'mcx> {
    pub concurrent: bool,
    pub skipData: bool,
    pub relation: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
}

#[derive(Default)]
pub struct CreateStmt<'mcx> {
    pub relation: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub tableElts: NodeList<'mcx>,
    pub inhRelations: NodeList<'mcx>,
    pub partbound: Option<Node<'mcx>>,
    pub partspec: Option<Node<'mcx>>,
    pub ofTypename: Option<Node<'mcx>>,
    pub constraints: NodeList<'mcx>,
    pub nnconstraints: NodeList<'mcx>,
    pub options: NodeList<'mcx>,
    pub oncommit: OnCommitAction,
    pub tablespacename: Option<&'mcx str>,
    pub accessMethod: Option<&'mcx str>,
    pub if_not_exists: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ViewCheckOption {
    #[default]
    NO_CHECK_OPTION = 0,
    LOCAL_CHECK_OPTION,
    CASCADED_CHECK_OPTION,
}

#[derive(Default)]
pub struct ViewStmt<'mcx> {
    pub view: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub aliases: NodeList<'mcx>,
    pub query: Option<Node<'mcx>>,
    pub replace: bool,
    pub options: NodeList<'mcx>,
    pub withCheckOption: ViewCheckOption,
}

pub struct RuleStmt<'mcx> {
    pub relation: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub rulename: &'mcx str,
    pub whereClause: Option<Node<'mcx>>,
    pub event: crate::nodes_enums::CmdType,
    pub instead: bool,
    pub actions: NodeList<'mcx>,
    pub replace: bool,
}

#[derive(Default)]
pub struct ColumnDef<'mcx> {
    pub colname: Option<&'mcx str>,
    pub typeName: Option<Node<'mcx>>,
    pub compression: Option<&'mcx str>,
    pub inhcount: i16,
    pub is_local: bool,
    pub is_not_null: bool,
    pub is_from_type: bool,
    pub storage: u8,
    pub storage_name: Option<&'mcx str>,
    pub raw_default: Option<Node<'mcx>>,
    pub cooked_default: Option<Node<'mcx>>,
    pub identity: u8,
    pub identitySequence: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub generated: u8,
    pub collClause: Option<Node<'mcx>>,
    pub collOid: Oid,
    pub constraints: NodeList<'mcx>,
    pub fdwoptions: NodeList<'mcx>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct PartitionElem<'mcx> {
    pub name: Option<&'mcx str>,
    pub expr: Option<Node<'mcx>>,
    pub collation: NodeList<'mcx>,
    pub opclass: NodeList<'mcx>,
    pub location: ParseLoc,
}

// C PartitionStrategy (parsenodes.h): char-valued.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum PartitionStrategy {
    #[default]
    List = b'l',
    Range = b'r',
    Hash = b'h',
}

#[derive(Default)]
pub struct PartitionSpec<'mcx> {
    pub strategy: PartitionStrategy,
    pub partParams: NodeList<'mcx>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct PartitionBoundSpec<'mcx> {
    pub strategy: u8,
    pub is_default: bool,
    pub modulus: i32,
    pub remainder: i32,
    pub listdatums: NodeList<'mcx>,
    pub lowerdatums: NodeList<'mcx>,
    pub upperdatums: NodeList<'mcx>,
    pub location: ParseLoc,
}

// C PartitionRangeDatumKind (parsenodes.h).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(i8)]
pub enum PartitionRangeDatumKind {
    Minvalue = -1,
    #[default]
    Value = 0,
    Maxvalue = 1,
}

#[derive(Default)]
pub struct PartitionRangeDatum<'mcx> {
    pub kind: PartitionRangeDatumKind,
    pub value: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct PartitionCmd<'mcx> {
    pub name: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub bound: Option<Node<'mcx>>,
    pub concurrent: bool,
}

// C ConstrType (parsenodes.h).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConstrType {
    #[default]
    CONSTR_NULL = 0,
    CONSTR_NOTNULL,
    CONSTR_DEFAULT,
    CONSTR_IDENTITY,
    CONSTR_GENERATED,
    CONSTR_CHECK,
    CONSTR_PRIMARY,
    CONSTR_UNIQUE,
    CONSTR_EXCLUSION,
    CONSTR_FOREIGN,
    CONSTR_ATTR_DEFERRABLE,
    CONSTR_ATTR_NOT_DEFERRABLE,
    CONSTR_ATTR_DEFERRED,
    CONSTR_ATTR_IMMEDIATE,
    CONSTR_ATTR_ENFORCED,
    CONSTR_ATTR_NOT_ENFORCED,
}

#[derive(Default)]
pub struct IndexElem<'mcx> {
    pub name: Option<&'mcx str>,
    pub expr: Option<Node<'mcx>>,
    pub indexcolname: Option<&'mcx str>,
    pub collation: NodeList<'mcx>,
    pub opclass: NodeList<'mcx>,
    pub opclassopts: NodeList<'mcx>,
    pub ordering: SortByDir,
    pub nulls_ordering: SortByNulls,
}

#[derive(Default)]
pub struct IndexStmt<'mcx> {
    pub idxname: Option<&'mcx str>,
    pub relation: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub accessMethod: Option<&'mcx str>,
    pub tableSpace: Option<&'mcx str>,
    pub indexParams: NodeList<'mcx>,
    pub indexIncludingParams: NodeList<'mcx>,
    pub options: NodeList<'mcx>,
    pub whereClause: Option<Node<'mcx>>,
    pub excludeOpNames: NodeList<'mcx>,
    pub idxcomment: Option<&'mcx str>,
    pub indexOid: Oid,
    pub oldNumber: types_core::RelFileNumber,
    pub oldCreateSubid: types_core::xact::SubTransactionId,
    pub oldFirstRelfilelocatorSubid: types_core::xact::SubTransactionId,
    pub unique: bool,
    pub nulls_not_distinct: bool,
    pub primary: bool,
    pub isconstraint: bool,
    pub iswithoutoverlaps: bool,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub transformed: bool,
    pub concurrent: bool,
    pub if_not_exists: bool,
    pub reset_default_tblspc: bool,
}

#[derive(Default)]
pub struct CreateSeqStmt<'mcx> {
    pub sequence: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub options: NodeList<'mcx>,
    pub ownerId: Oid,
    pub for_identity: bool,
    pub if_not_exists: bool,
}

#[derive(Default)]
pub struct AlterSeqStmt<'mcx> {
    pub sequence: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub options: NodeList<'mcx>,
    pub for_identity: bool,
    pub missing_ok: bool,
}

#[derive(Default)]
pub struct TableLikeClause<'mcx> {
    pub relation: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub options: u32,
    pub relationOid: Oid,
}

pub const CREATE_TABLE_LIKE_COMMENTS: u32 = 1 << 0;
pub const CREATE_TABLE_LIKE_COMPRESSION: u32 = 1 << 1;
pub const CREATE_TABLE_LIKE_CONSTRAINTS: u32 = 1 << 2;
pub const CREATE_TABLE_LIKE_DEFAULTS: u32 = 1 << 3;
pub const CREATE_TABLE_LIKE_GENERATED: u32 = 1 << 4;
pub const CREATE_TABLE_LIKE_IDENTITY: u32 = 1 << 5;
pub const CREATE_TABLE_LIKE_INDEXES: u32 = 1 << 6;
pub const CREATE_TABLE_LIKE_STATISTICS: u32 = 1 << 7;
pub const CREATE_TABLE_LIKE_STORAGE: u32 = 1 << 8;
pub const CREATE_TABLE_LIKE_ALL: u32 = i32::MAX as u32;

// DEFAULT/CHECK slice of C's Constraint; index/FK fields arrive with their DDL.
pub const FKCONSTR_ACTION_NOACTION: u8 = b'a';
pub const FKCONSTR_ACTION_RESTRICT: u8 = b'r';
pub const FKCONSTR_ACTION_CASCADE: u8 = b'c';
pub const FKCONSTR_ACTION_SETNULL: u8 = b'n';
pub const FKCONSTR_ACTION_SETDEFAULT: u8 = b'd';

pub const FKCONSTR_MATCH_FULL: u8 = b'f';
pub const FKCONSTR_MATCH_PARTIAL: u8 = b'p';
pub const FKCONSTR_MATCH_SIMPLE: u8 = b's';

#[derive(Default)]
pub struct Constraint<'mcx> {
    pub contype: ConstrType,
    pub conname: Option<&'mcx str>,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub is_enforced: bool,
    pub skip_validation: bool,
    pub initially_valid: bool,
    pub is_no_inherit: bool,
    pub raw_expr: Option<Node<'mcx>>,
    pub cooked_expr: Option<&'mcx str>,
    pub generated_when: u8,
    pub generated_kind: u8,
    pub nulls_not_distinct: bool,
    pub keys: NodeList<'mcx>,
    pub without_overlaps: bool,
    pub including: NodeList<'mcx>,
    pub exclusions: NodeList<'mcx>,
    pub options: NodeList<'mcx>,
    pub access_method: Option<&'mcx str>,
    pub where_clause: Option<Node<'mcx>>,
    pub indexname: Option<&'mcx str>,
    pub indexspace: Option<&'mcx str>,
    pub pktable: Option<&'mcx crate::RangeVar<'mcx>>,
    pub fk_attrs: NodeList<'mcx>,
    pub pk_attrs: NodeList<'mcx>,
    pub fk_with_period: bool,
    pub pk_with_period: bool,
    pub fk_matchtype: u8,
    pub fk_upd_action: u8,
    pub fk_del_action: u8,
    pub fk_del_set_cols: NodeList<'mcx>,
    pub old_conpfeqop: OidList<'mcx>,
    pub old_pktable_oid: Oid,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct CreateDomainStmt<'mcx> {
    pub domainname: NodeList<'mcx>,
    pub typeName: Option<Node<'mcx>>,
    pub collClause: Option<Node<'mcx>>,
    pub constraints: NodeList<'mcx>,
}

#[derive(Default)]
pub struct CreateEnumStmt<'mcx> {
    pub typeName: NodeList<'mcx>,
    pub vals: NodeList<'mcx>,
}

#[derive(Default)]
pub struct CreateRangeStmt<'mcx> {
    pub typeName: NodeList<'mcx>,
    pub params: NodeList<'mcx>,
}

#[derive(Default)]
pub struct AlterTypeStmt<'mcx> {
    pub typeName: NodeList<'mcx>,
    pub options: NodeList<'mcx>,
}

#[derive(Default)]
pub struct AlterEnumStmt<'mcx> {
    pub typeName: NodeList<'mcx>,
    pub oldVal: Option<&'mcx str>,
    pub newVal: Option<&'mcx str>,
    pub newValNeighbor: Option<&'mcx str>,
    pub newValIsAfter: bool,
    pub skipIfNewValExists: bool,
}

/// `val` is a SelectStmt node (the PLpgSQL_Expr production's result).
#[derive(Default)]
pub struct PLAssignStmt<'mcx> {
    pub name: &'mcx str,
    pub indirection: NodeList<'mcx>,
    pub nnames: i32,
    pub val: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

/// `funcexpr`/`outargs` are filled by transform (parse_analyze), not the grammar.
#[derive(Default)]
pub struct CallStmt<'mcx> {
    pub funccall: Option<&'mcx FuncCall<'mcx>>,
    pub funcexpr: Option<&'mcx crate::primnodes::FuncExpr<'mcx>>,
    pub outargs: NodeList<'mcx>,
}

// SAFETY (each): tag/type pairing mirrors parsenodes.h.
unsafe impl<'mcx> NodeVariant<'mcx> for CallStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CallStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RawStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_RawStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for PLAssignStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_PLAssignStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CompositeTypeStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CompositeTypeStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterTSDictionaryStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterTSDictionaryStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterTSConfigurationStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterTSConfigurationStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateDomainStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateDomainStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateEnumStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateEnumStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateRangeStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateRangeStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterTypeStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterTypeStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterEnumStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterEnumStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SelectStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_SelectStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for IntoClause<'mcx> {
    const TAG: NodeTag = NodeTag::T_IntoClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateTableAsStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateTableAsStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RefreshMatViewStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_RefreshMatViewStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for InsertStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_InsertStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ReturningOption<'mcx> {
    const TAG: NodeTag = NodeTag::T_ReturningOption;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ReturningClause<'mcx> {
    const TAG: NodeTag = NodeTag::T_ReturningClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for MergeStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_MergeStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for MergeWhenClause<'mcx> {
    const TAG: NodeTag = NodeTag::T_MergeWhenClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for OnConflictClause<'mcx> {
    const TAG: NodeTag = NodeTag::T_OnConflictClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for InferClause<'mcx> {
    const TAG: NodeTag = NodeTag::T_InferClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for DeleteStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_DeleteStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for UpdateStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_UpdateStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ResTarget<'mcx> {
    const TAG: NodeTag = NodeTag::T_ResTarget;
}
unsafe impl<'mcx> NodeVariant<'mcx> for A_Expr<'mcx> {
    const TAG: NodeTag = NodeTag::T_A_Expr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for A_Const<'mcx> {
    const TAG: NodeTag = NodeTag::T_A_Const;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CollateClause<'mcx> {
    const TAG: NodeTag = NodeTag::T_CollateClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ColumnRef<'mcx> {
    const TAG: NodeTag = NodeTag::T_ColumnRef;
}
unsafe impl NodeVariant<'_> for ParamRef {
    const TAG: NodeTag = NodeTag::T_ParamRef;
}
unsafe impl NodeVariant<'_> for A_Star {
    const TAG: NodeTag = NodeTag::T_A_Star;
}
unsafe impl<'mcx> NodeVariant<'mcx> for A_Indices<'mcx> {
    const TAG: NodeTag = NodeTag::T_A_Indices;
}
unsafe impl<'mcx> NodeVariant<'mcx> for A_Indirection<'mcx> {
    const TAG: NodeTag = NodeTag::T_A_Indirection;
}
unsafe impl<'mcx> NodeVariant<'mcx> for A_ArrayExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_A_ArrayExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for MultiAssignRef<'mcx> {
    const TAG: NodeTag = NodeTag::T_MultiAssignRef;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SortBy<'mcx> {
    const TAG: NodeTag = NodeTag::T_SortBy;
}
unsafe impl<'mcx> NodeVariant<'mcx> for WindowDef<'mcx> {
    const TAG: NodeTag = NodeTag::T_WindowDef;
}
unsafe impl<'mcx> NodeVariant<'mcx> for FuncCall<'mcx> {
    const TAG: NodeTag = NodeTag::T_FuncCall;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RangeSubselect<'mcx> {
    const TAG: NodeTag = NodeTag::T_RangeSubselect;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RangeFunction<'mcx> {
    const TAG: NodeTag = NodeTag::T_RangeFunction;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RangeTableSample<'mcx> {
    const TAG: NodeTag = NodeTag::T_RangeTableSample;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RangeTableFunc<'mcx> {
    const TAG: NodeTag = NodeTag::T_RangeTableFunc;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RangeTableFuncCol<'mcx> {
    const TAG: NodeTag = NodeTag::T_RangeTableFuncCol;
}
unsafe impl<'mcx> NodeVariant<'mcx> for XmlSerialize<'mcx> {
    const TAG: NodeTag = NodeTag::T_XmlSerialize;
}
unsafe impl<'mcx> NodeVariant<'mcx> for TypeName<'mcx> {
    const TAG: NodeTag = NodeTag::T_TypeName;
}
unsafe impl<'mcx> NodeVariant<'mcx> for TypeCast<'mcx> {
    const TAG: NodeTag = NodeTag::T_TypeCast;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ViewStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_ViewStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RuleStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_RuleStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ColumnDef<'mcx> {
    const TAG: NodeTag = NodeTag::T_ColumnDef;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Constraint<'mcx> {
    const TAG: NodeTag = NodeTag::T_Constraint;
}
unsafe impl<'mcx> NodeVariant<'mcx> for IndexElem<'mcx> {
    const TAG: NodeTag = NodeTag::T_IndexElem;
}
unsafe impl<'mcx> NodeVariant<'mcx> for IndexStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_IndexStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for TableLikeClause<'mcx> {
    const TAG: NodeTag = NodeTag::T_TableLikeClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for LockingClause<'mcx> {
    const TAG: NodeTag = NodeTag::T_LockingClause;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateSeqStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateSeqStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterSeqStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterSeqStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for PartitionElem<'mcx> {
    const TAG: NodeTag = NodeTag::T_PartitionElem;
}
unsafe impl<'mcx> NodeVariant<'mcx> for PartitionSpec<'mcx> {
    const TAG: NodeTag = NodeTag::T_PartitionSpec;
}
unsafe impl<'mcx> NodeVariant<'mcx> for PartitionBoundSpec<'mcx> {
    const TAG: NodeTag = NodeTag::T_PartitionBoundSpec;
}
unsafe impl<'mcx> NodeVariant<'mcx> for PartitionRangeDatum<'mcx> {
    const TAG: NodeTag = NodeTag::T_PartitionRangeDatum;
}
unsafe impl<'mcx> NodeVariant<'mcx> for PartitionCmd<'mcx> {
    const TAG: NodeTag = NodeTag::T_PartitionCmd;
}

#[derive(Default)]
pub struct CreateStatsStmt<'mcx> {
    pub defnames: NodeList<'mcx>,
    pub stat_types: NodeList<'mcx>,
    pub exprs: NodeList<'mcx>,
    pub relations: NodeList<'mcx>,
    pub stxcomment: Option<&'mcx str>,
    pub transformed: bool,
    pub if_not_exists: bool,
}

#[derive(Default)]
pub struct StatsElem<'mcx> {
    pub name: Option<&'mcx str>,
    pub expr: Option<Node<'mcx>>,
}

unsafe impl<'mcx> NodeVariant<'mcx> for CreateStatsStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateStatsStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for StatsElem<'mcx> {
    const TAG: NodeTag = NodeTag::T_StatsElem;
}

#[derive(Default)]
pub struct AlterStatsStmt<'mcx> {
    pub defnames: NodeList<'mcx>,
    // Integer node; None = SET STATISTICS DEFAULT.
    pub stxstattarget: Option<Node<'mcx>>,
    pub missing_ok: bool,
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterStatsStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterStatsStmt;
}

// timing/events use the TRIGGER_TYPE bits of catalog/pg_trigger.h.
#[derive(Default)]
pub struct CreateTrigStmt<'mcx> {
    pub replace: bool,
    pub isconstraint: bool,
    pub trigname: Option<&'mcx str>,
    pub relation: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub funcname: NodeList<'mcx>,
    pub args: NodeList<'mcx>,
    pub row: bool,
    pub timing: i16,
    pub events: i16,
    pub columns: NodeList<'mcx>,
    pub whenClause: Option<Node<'mcx>>,
    pub transitionRels: NodeList<'mcx>,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub constrrel: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
}

#[derive(Default)]
pub struct TriggerTransition<'mcx> {
    pub name: Option<&'mcx str>,
    pub isNew: bool,
    pub isTable: bool,
}

#[derive(Default)]
pub struct ConstraintsSetStmt<'mcx> {
    pub constraints: NodeList<'mcx>,
    pub deferred: bool,
}

unsafe impl<'mcx> NodeVariant<'mcx> for CreateTrigStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateTrigStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for TriggerTransition<'mcx> {
    const TAG: NodeTag = NodeTag::T_TriggerTransition;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ConstraintsSetStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_ConstraintsSetStmt;
}

#[derive(Default)]
pub struct CreateExtensionStmt<'mcx> {
    pub extname: Option<&'mcx str>,
    pub if_not_exists: bool,
    pub options: NodeList<'mcx>,
}

#[derive(Default)]
pub struct AlterExtensionStmt<'mcx> {
    pub extname: Option<&'mcx str>,
    pub options: NodeList<'mcx>,
}

unsafe impl<'mcx> NodeVariant<'mcx> for CreateExtensionStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateExtensionStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterExtensionStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterExtensionStmt;
}

#[derive(Default)]
pub struct AlterExtensionContentsStmt<'mcx> {
    pub extname: Option<&'mcx str>,
    // +1 = add object, -1 = drop object.
    pub action: i32,
    pub objtype: crate::parsenodes::ObjectType,
    pub object: Option<Node<'mcx>>,
}

unsafe impl<'mcx> NodeVariant<'mcx> for AlterExtensionContentsStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterExtensionContentsStmt;
}

impl<'mcx> Node<'mcx> {
    pub fn mk_raw_stmt(
        mcx: Mcx<'mcx>,
        stmt: Option<Node<'mcx>>,
        stmt_location: ParseLoc,
        stmt_len: ParseLoc,
    ) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, RawStmt { stmt, stmt_location, stmt_len })
    }

    pub fn mk_res_target(
        mcx: Mcx<'mcx>,
        name: Option<&'mcx str>,
        indirection: NodeList<'mcx>,
        val: Option<Node<'mcx>>,
        location: ParseLoc,
    ) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, ResTarget { name, indirection, val, location })
    }

    /// C `makeA_Expr` (rexpr list bounds start unset).
    pub fn mk_a_expr(
        mcx: Mcx<'mcx>,
        kind: A_Expr_Kind,
        name: NodeList<'mcx>,
        lexpr: Option<Node<'mcx>>,
        rexpr: Option<Node<'mcx>>,
        location: ParseLoc,
    ) -> PgResult<Node<'mcx>> {
        Self::mk(
            mcx,
            A_Expr { kind, name, lexpr, rexpr, rexpr_list_start: -1, rexpr_list_end: -1, location },
        )
    }

    pub fn mk_a_const(
        mcx: Mcx<'mcx>,
        val: Option<ValUnion<'mcx>>,
        location: ParseLoc,
    ) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, A_Const { val, location })
    }

    pub fn mk_column_ref(
        mcx: Mcx<'mcx>,
        fields: NodeList<'mcx>,
        location: ParseLoc,
    ) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, ColumnRef { fields, location })
    }

    pub fn mk_param_ref(mcx: Mcx<'mcx>, number: i32, location: ParseLoc) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, ParamRef { number, location })
    }

    pub fn mk_a_star(mcx: Mcx<'mcx>) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, A_Star)
    }

    #[inline]
    pub fn as_raw_stmt(self) -> Option<&'mcx RawStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_pl_assign_stmt(self) -> Option<&'mcx PLAssignStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_call_stmt(self) -> Option<&'mcx CallStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_select_stmt(self) -> Option<&'mcx SelectStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_insert_stmt(self) -> Option<&'mcx InsertStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_returning_clause(self) -> Option<&'mcx ReturningClause<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_on_conflict_clause(self) -> Option<&'mcx OnConflictClause<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_merge_stmt(self) -> Option<&'mcx MergeStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_merge_when_clause(self) -> Option<&'mcx MergeWhenClause<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_infer_clause(self) -> Option<&'mcx InferClause<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_delete_stmt(self) -> Option<&'mcx DeleteStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_update_stmt(self) -> Option<&'mcx UpdateStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_res_target(self) -> Option<&'mcx ResTarget<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_a_expr(self) -> Option<&'mcx A_Expr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_a_const(self) -> Option<&'mcx A_Const<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_collate_clause(self) -> Option<&'mcx CollateClause<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_column_ref(self) -> Option<&'mcx ColumnRef<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_param_ref(self) -> Option<&'mcx ParamRef> {
        self.as_variant()
    }

    #[inline]
    pub fn as_a_star(self) -> Option<&'mcx A_Star> {
        self.as_variant()
    }

    #[inline]
    pub fn as_sort_by(self) -> Option<&'mcx SortBy<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_window_def(self) -> Option<&'mcx WindowDef<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_range_function(self) -> Option<&'mcx RangeFunction<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_range_table_sample(self) -> Option<&'mcx RangeTableSample<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_range_subselect(self) -> Option<&'mcx RangeSubselect<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_range_table_func(self) -> Option<&'mcx RangeTableFunc<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_range_table_func_col(self) -> Option<&'mcx RangeTableFuncCol<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_xml_serialize(self) -> Option<&'mcx XmlSerialize<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_func_call(self) -> Option<&'mcx FuncCall<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_type_name(self) -> Option<&'mcx TypeName<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_type_cast(self) -> Option<&'mcx TypeCast<'mcx>> {
        self.as_variant()
    }


    #[inline]
    pub fn as_locking_clause(self) -> Option<&'mcx LockingClause<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_create_seq_stmt(self) -> Option<&'mcx CreateSeqStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_create_enum_stmt(self) -> Option<&'mcx CreateEnumStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_create_range_stmt(self) -> Option<&'mcx CreateRangeStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_alter_seq_stmt(self) -> Option<&'mcx AlterSeqStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_alter_type_stmt(self) -> Option<&'mcx AlterTypeStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_alter_enum_stmt(self) -> Option<&'mcx AlterEnumStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_create_domain_stmt(self) -> Option<&'mcx CreateDomainStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_create_trig_stmt(self) -> Option<&'mcx CreateTrigStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_a_indices(self) -> Option<&'mcx A_Indices<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_composite_type_stmt(self) -> Option<&'mcx CompositeTypeStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_trigger_transition(self) -> Option<&'mcx TriggerTransition<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_alter_ts_dictionary_stmt(self) -> Option<&'mcx AlterTSDictionaryStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_a_indirection(self) -> Option<&'mcx A_Indirection<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_constraints_set_stmt(self) -> Option<&'mcx ConstraintsSetStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_alter_ts_configuration_stmt(self) -> Option<&'mcx AlterTSConfigurationStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_a_array_expr(self) -> Option<&'mcx A_ArrayExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_multi_assign_ref(self) -> Option<&'mcx MultiAssignRef<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_into_clause(self) -> Option<&'mcx IntoClause<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_column_def(self) -> Option<&'mcx ColumnDef<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_index_elem(self) -> Option<&'mcx IndexElem<'mcx>> {
        self.as_variant()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum JsonQuotes {
    #[default]
    JS_QUOTES_UNSPEC = 0,
    JS_QUOTES_KEEP = 1,
    JS_QUOTES_OMIT = 2,
}

#[derive(Default)]
pub struct JsonOutput<'mcx> {
    pub typeName: Option<Node<'mcx>>,
    pub returning: Option<&'mcx crate::primnodes::JsonReturning<'mcx>>,
}

#[derive(Default)]
pub struct JsonArgument<'mcx> {
    pub val: Option<Node<'mcx>>,
    pub name: Option<&'mcx str>,
}

pub struct JsonFuncExpr<'mcx> {
    pub op: crate::primnodes::JsonExprOp,
    pub column_name: Option<&'mcx str>,
    pub context_item: Option<Node<'mcx>>,
    pub pathspec: Option<Node<'mcx>>,
    pub passing: NodeList<'mcx>,
    pub output: Option<Node<'mcx>>,
    pub on_empty: Option<Node<'mcx>>,
    pub on_error: Option<Node<'mcx>>,
    pub wrapper: crate::primnodes::JsonWrapper,
    pub quotes: JsonQuotes,
    pub location: ParseLoc,
}

impl Default for JsonFuncExpr<'_> {
    fn default() -> Self {
        JsonFuncExpr {
            op: crate::primnodes::JsonExprOp::JSON_EXISTS_OP,
            column_name: None,
            context_item: None,
            pathspec: None,
            passing: NodeList::nil(),
            output: None,
            on_empty: None,
            on_error: None,
            wrapper: crate::primnodes::JsonWrapper::JSW_UNSPEC,
            quotes: JsonQuotes::JS_QUOTES_UNSPEC,
            location: -1,
        }
    }
}

// `string` is the A_Const String path literal (makeStringConst shape).
pub struct JsonTablePathSpec<'mcx> {
    pub string: Option<Node<'mcx>>,
    pub name: Option<&'mcx str>,
    pub name_location: ParseLoc,
    pub location: ParseLoc,
}

impl Default for JsonTablePathSpec<'_> {
    fn default() -> Self {
        JsonTablePathSpec { string: None, name: None, name_location: -1, location: -1 }
    }
}

pub struct JsonTable<'mcx> {
    pub context_item: Option<Node<'mcx>>,
    pub pathspec: Option<Node<'mcx>>,
    pub passing: NodeList<'mcx>,
    pub columns: NodeList<'mcx>,
    pub on_error: Option<Node<'mcx>>,
    pub alias: Option<&'mcx crate::primnodes::Alias<'mcx>>,
    pub lateral: bool,
    pub location: ParseLoc,
}

impl Default for JsonTable<'_> {
    fn default() -> Self {
        JsonTable {
            context_item: None,
            pathspec: None,
            passing: NodeList::nil(),
            columns: NodeList::nil(),
            on_error: None,
            alias: None,
            lateral: false,
            location: -1,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum JsonTableColumnType {
    #[default]
    JTC_FOR_ORDINALITY = 0,
    JTC_REGULAR = 1,
    JTC_EXISTS = 2,
    JTC_FORMATTED = 3,
    JTC_NESTED = 4,
}

pub struct JsonTableColumn<'mcx> {
    pub coltype: JsonTableColumnType,
    pub name: Option<&'mcx str>,
    pub typeName: Option<Node<'mcx>>,
    pub pathspec: Option<Node<'mcx>>,
    pub format: Option<&'mcx crate::primnodes::JsonFormat>,
    pub wrapper: crate::primnodes::JsonWrapper,
    pub quotes: JsonQuotes,
    pub columns: NodeList<'mcx>,
    pub on_empty: Option<Node<'mcx>>,
    pub on_error: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

impl Default for JsonTableColumn<'_> {
    fn default() -> Self {
        JsonTableColumn {
            coltype: JsonTableColumnType::JTC_FOR_ORDINALITY,
            name: None,
            typeName: None,
            pathspec: None,
            format: None,
            wrapper: crate::primnodes::JsonWrapper::JSW_UNSPEC,
            quotes: JsonQuotes::JS_QUOTES_UNSPEC,
            columns: NodeList::nil(),
            on_empty: None,
            on_error: None,
            location: -1,
        }
    }
}

#[derive(Default)]
pub struct JsonKeyValue<'mcx> {
    pub key: Option<Node<'mcx>>,
    pub value: Option<Node<'mcx>>,
}

pub struct JsonParseExpr<'mcx> {
    pub expr: Option<Node<'mcx>>,
    pub output: Option<Node<'mcx>>,
    pub unique_keys: bool,
    pub location: ParseLoc,
}

impl Default for JsonParseExpr<'_> {
    fn default() -> Self {
        JsonParseExpr { expr: None, output: None, unique_keys: false, location: -1 }
    }
}

pub struct JsonScalarExpr<'mcx> {
    pub expr: Option<Node<'mcx>>,
    pub output: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

impl Default for JsonScalarExpr<'_> {
    fn default() -> Self {
        JsonScalarExpr { expr: None, output: None, location: -1 }
    }
}

pub struct JsonSerializeExpr<'mcx> {
    pub expr: Option<Node<'mcx>>,
    pub output: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

impl Default for JsonSerializeExpr<'_> {
    fn default() -> Self {
        JsonSerializeExpr { expr: None, output: None, location: -1 }
    }
}

pub struct JsonObjectConstructor<'mcx> {
    pub exprs: NodeList<'mcx>,
    pub output: Option<Node<'mcx>>,
    pub absent_on_null: bool,
    pub unique: bool,
    pub location: ParseLoc,
}

impl Default for JsonObjectConstructor<'_> {
    fn default() -> Self {
        JsonObjectConstructor {
            exprs: NodeList::nil(),
            output: None,
            absent_on_null: false,
            unique: false,
            location: -1,
        }
    }
}

pub struct JsonArrayConstructor<'mcx> {
    pub exprs: NodeList<'mcx>,
    pub output: Option<Node<'mcx>>,
    pub absent_on_null: bool,
    pub location: ParseLoc,
}

impl Default for JsonArrayConstructor<'_> {
    fn default() -> Self {
        JsonArrayConstructor {
            exprs: NodeList::nil(),
            output: None,
            absent_on_null: false,
            location: -1,
        }
    }
}

pub struct JsonArrayQueryConstructor<'mcx> {
    pub query: Option<Node<'mcx>>,
    pub output: Option<Node<'mcx>>,
    pub format: Option<&'mcx crate::primnodes::JsonFormat>,
    pub absent_on_null: bool,
    pub location: ParseLoc,
}

impl Default for JsonArrayQueryConstructor<'_> {
    fn default() -> Self {
        JsonArrayQueryConstructor {
            query: None,
            output: None,
            format: None,
            absent_on_null: false,
            location: -1,
        }
    }
}

pub struct JsonAggConstructor<'mcx> {
    pub output: Option<Node<'mcx>>,
    pub agg_filter: Option<Node<'mcx>>,
    pub agg_order: NodeList<'mcx>,
    pub over: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

impl Default for JsonAggConstructor<'_> {
    fn default() -> Self {
        JsonAggConstructor {
            output: None,
            agg_filter: None,
            agg_order: NodeList::nil(),
            over: None,
            location: -1,
        }
    }
}

#[derive(Default)]
pub struct JsonObjectAgg<'mcx> {
    pub constructor: Option<Node<'mcx>>,
    pub arg: Option<Node<'mcx>>,
    pub absent_on_null: bool,
    pub unique: bool,
}

#[derive(Default)]
pub struct JsonArrayAgg<'mcx> {
    pub constructor: Option<Node<'mcx>>,
    pub arg: Option<Node<'mcx>>,
    pub absent_on_null: bool,
}

// SAFETY (each): tag/type pairing mirrors parsenodes.h.
unsafe impl<'mcx> NodeVariant<'mcx> for JsonOutput<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonOutput;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonArgument<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonArgument;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonFuncExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonFuncExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonTablePathSpec<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonTablePathSpec;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonTable<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonTable;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonTableColumn<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonTableColumn;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonKeyValue<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonKeyValue;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonParseExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonParseExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonScalarExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonScalarExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonSerializeExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonSerializeExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonObjectConstructor<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonObjectConstructor;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonArrayConstructor<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonArrayConstructor;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonArrayQueryConstructor<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonArrayQueryConstructor;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonAggConstructor<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonAggConstructor;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonObjectAgg<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonObjectAgg;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonArrayAgg<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonArrayAgg;
}

impl<'mcx> Node<'mcx> {
    #[inline]
    pub fn as_json_output(self) -> Option<&'mcx JsonOutput<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_argument(self) -> Option<&'mcx JsonArgument<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_func_expr(self) -> Option<&'mcx JsonFuncExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_table(self) -> Option<&'mcx JsonTable<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_table_column(self) -> Option<&'mcx JsonTableColumn<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_table_path_spec(self) -> Option<&'mcx JsonTablePathSpec<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_key_value(self) -> Option<&'mcx JsonKeyValue<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_parse_expr(self) -> Option<&'mcx JsonParseExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_scalar_expr(self) -> Option<&'mcx JsonScalarExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_serialize_expr(self) -> Option<&'mcx JsonSerializeExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_object_constructor(self) -> Option<&'mcx JsonObjectConstructor<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_array_constructor(self) -> Option<&'mcx JsonArrayConstructor<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_array_query_constructor(self) -> Option<&'mcx JsonArrayQueryConstructor<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_agg_constructor(self) -> Option<&'mcx JsonAggConstructor<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_object_agg(self) -> Option<&'mcx JsonObjectAgg<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_array_agg(self) -> Option<&'mcx JsonArrayAgg<'mcx>> {
        self.as_variant()
    }
}
#[derive(Default)]
pub struct CreateFdwStmt<'mcx> {
    pub fdwname: Option<&'mcx str>,
    pub func_options: NodeList<'mcx>,
    pub options: NodeList<'mcx>,
}

#[derive(Default)]
pub struct AlterFdwStmt<'mcx> {
    pub fdwname: Option<&'mcx str>,
    pub func_options: NodeList<'mcx>,
    pub options: NodeList<'mcx>,
}

#[derive(Default)]
pub struct CreateForeignServerStmt<'mcx> {
    pub servername: Option<&'mcx str>,
    pub servertype: Option<&'mcx str>,
    pub version: Option<&'mcx str>,
    pub fdwname: Option<&'mcx str>,
    pub if_not_exists: bool,
    pub options: NodeList<'mcx>,
}

#[derive(Default)]
pub struct AlterForeignServerStmt<'mcx> {
    pub servername: Option<&'mcx str>,
    pub version: Option<&'mcx str>,
    pub options: NodeList<'mcx>,
    pub has_version: bool,
}

#[derive(Default)]
pub struct CreateForeignTableStmt<'mcx> {
    pub base: CreateStmt<'mcx>,
    pub servername: Option<&'mcx str>,
    pub options: NodeList<'mcx>,
}

#[derive(Default)]
pub struct CreateUserMappingStmt<'mcx> {
    pub user: Option<&'mcx crate::parsenodes::RoleSpec<'mcx>>,
    pub servername: Option<&'mcx str>,
    pub if_not_exists: bool,
    pub options: NodeList<'mcx>,
}

#[derive(Default)]
pub struct AlterUserMappingStmt<'mcx> {
    pub user: Option<&'mcx crate::parsenodes::RoleSpec<'mcx>>,
    pub servername: Option<&'mcx str>,
    pub options: NodeList<'mcx>,
}

#[derive(Default)]
pub struct DropUserMappingStmt<'mcx> {
    pub user: Option<&'mcx crate::parsenodes::RoleSpec<'mcx>>,
    pub servername: Option<&'mcx str>,
    pub missing_ok: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum ImportForeignSchemaType {
    #[default]
    FDW_IMPORT_SCHEMA_ALL = 0,
    FDW_IMPORT_SCHEMA_LIMIT_TO = 1,
    FDW_IMPORT_SCHEMA_EXCEPT = 2,
}

#[derive(Default)]
pub struct ImportForeignSchemaStmt<'mcx> {
    pub server_name: Option<&'mcx str>,
    pub remote_schema: Option<&'mcx str>,
    pub local_schema: Option<&'mcx str>,
    pub list_type: ImportForeignSchemaType,
    pub table_list: NodeList<'mcx>,
    pub options: NodeList<'mcx>,
}

unsafe impl<'mcx> NodeVariant<'mcx> for CreateFdwStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateFdwStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterFdwStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterFdwStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateForeignServerStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateForeignServerStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterForeignServerStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterForeignServerStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateForeignTableStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateForeignTableStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateUserMappingStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateUserMappingStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlterUserMappingStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlterUserMappingStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for DropUserMappingStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_DropUserMappingStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ImportForeignSchemaStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_ImportForeignSchemaStmt;
}

