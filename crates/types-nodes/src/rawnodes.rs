//! Full parse-tree producer model + raw-grammar input nodes
//! (`nodes/parsenodes.h` "Supporting data structures for Parse Trees" + the raw
//! statement sections, plus the `nodes/primnodes.h` `Alias`/`RangeVar`/
//! `JoinExpr`/`FromExpr`/`OnConflictExpr`/`MergeAction` join/clause nodes).
//!
//! This is the K1-parsetree keystone: the full *producer* statement-node
//! vocabulary the parser/analyze/rewrite/planner emit, and the raw-grammar
//! INPUT nodes `analyze.c`/`parse_clause.c`/`parse_expr.c` consume. Earlier
//! milestones carried only the consumer-trimmed subset
//! ([`crate::copy_query::Query`], [`crate::parsenodes::RangeTblEntry`]); those
//! views remain (additive), this module supplies the rest.
//!
//! Modelling rules (docs/types.md): `Node *` subtrees are owned
//! `PgBox<'mcx, Node<'mcx>>`; `List *` of nodes are `PgVec<'mcx, …>`; `char *`
//! are `PgString<'mcx>`; expression subtrees ride the canonical owned
//! [`crate::primnodes::Expr`] through [`crate::nodes::Node::Expr`] — NO handles
//! (opacity-inherited-never-introduced; parse trees are owned, not aliased).

use mcx::{Mcx, PgBox, PgString, PgVec};
use types_core::primitive::{Index, Oid};
use types_error::PgResult;

use crate::jointype::JoinType;
use crate::nodelimit::LimitOption;
use crate::nodes::{CmdType, NodePtr, OnConflictAction};
use crate::primnodes::CoercionForm;

// ===========================================================================
// Small grammar enums (nodes/parsenodes.h, nodes/nodes.h)
// ===========================================================================

/// `SortByDir` (`nodes/parsenodes.h`) — ASC/DESC/USING for ORDER BY/index.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum SortByDir {
    #[default]
    SORTBY_DEFAULT = 0,
    SORTBY_ASC = 1,
    SORTBY_DESC = 2,
    SORTBY_USING = 3,
}
pub use SortByDir::{SORTBY_ASC, SORTBY_DEFAULT, SORTBY_DESC, SORTBY_USING};

/// `SortByNulls` (`nodes/parsenodes.h`) — NULLS FIRST/LAST.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum SortByNulls {
    #[default]
    SORTBY_NULLS_DEFAULT = 0,
    SORTBY_NULLS_FIRST = 1,
    SORTBY_NULLS_LAST = 2,
}
pub use SortByNulls::{SORTBY_NULLS_DEFAULT, SORTBY_NULLS_FIRST, SORTBY_NULLS_LAST};

/// `LockClauseStrength` (`nodes/lockoptions.h`) — FOR UPDATE/SHARE strength.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum LockClauseStrength {
    /// no such clause - only used in PlanRowMark
    #[default]
    LCS_NONE = 0,
    LCS_FORKEYSHARE = 1,
    LCS_FORSHARE = 2,
    LCS_FORNOKEYUPDATE = 3,
    LCS_FORUPDATE = 4,
}
pub use LockClauseStrength::{
    LCS_FORKEYSHARE, LCS_FORNOKEYUPDATE, LCS_FORSHARE, LCS_FORUPDATE, LCS_NONE,
};

/// `LockWaitPolicy` (`nodes/lockoptions.h`) — NOWAIT / SKIP LOCKED.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum LockWaitPolicy {
    /// Wait for the lock to become available (default behavior)
    #[default]
    LockWaitBlock = 0,
    /// Skip rows that can't be locked (SKIP LOCKED)
    LockWaitSkip = 1,
    /// Raise an error if a row cannot be locked (NOWAIT)
    LockWaitError = 2,
}
pub use LockWaitPolicy::{LockWaitBlock, LockWaitError, LockWaitSkip};

/// `SetOperation` (`nodes/parsenodes.h`) — UNION/INTERSECT/EXCEPT op type.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum SetOperation {
    #[default]
    SETOP_NONE = 0,
    SETOP_UNION = 1,
    SETOP_INTERSECT = 2,
    SETOP_EXCEPT = 3,
}
pub use SetOperation::{SETOP_EXCEPT, SETOP_INTERSECT, SETOP_NONE, SETOP_UNION};

/// `GroupingSetKind` (`nodes/parsenodes.h`) — CUBE/ROLLUP/GROUPING SETS kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum GroupingSetKind {
    #[default]
    GROUPING_SET_EMPTY = 0,
    GROUPING_SET_SIMPLE = 1,
    GROUPING_SET_ROLLUP = 2,
    GROUPING_SET_CUBE = 3,
    GROUPING_SET_SETS = 4,
}
pub use GroupingSetKind::{
    GROUPING_SET_CUBE, GROUPING_SET_EMPTY, GROUPING_SET_ROLLUP, GROUPING_SET_SETS,
    GROUPING_SET_SIMPLE,
};

/// `CTEMaterialize` (`nodes/parsenodes.h`) — MATERIALIZED / NOT MATERIALIZED.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum CTEMaterialize {
    #[default]
    CTEMaterializeDefault = 0,
    CTEMaterializeAlways = 1,
    CTEMaterializeNever = 2,
}
pub use CTEMaterialize::{CTEMaterializeAlways, CTEMaterializeDefault, CTEMaterializeNever};

/// `WCOKind` (`nodes/parsenodes.h`) — WITH CHECK OPTION / RLS check kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum WCOKind {
    #[default]
    WCO_VIEW_CHECK = 0,
    WCO_RLS_INSERT_CHECK = 1,
    WCO_RLS_UPDATE_CHECK = 2,
    WCO_RLS_CONFLICT_CHECK = 3,
    WCO_RLS_MERGE_UPDATE_CHECK = 4,
    WCO_RLS_MERGE_DELETE_CHECK = 5,
}
pub use WCOKind::{
    WCO_RLS_CONFLICT_CHECK, WCO_RLS_INSERT_CHECK, WCO_RLS_MERGE_DELETE_CHECK,
    WCO_RLS_MERGE_UPDATE_CHECK, WCO_RLS_UPDATE_CHECK, WCO_VIEW_CHECK,
};

/// `A_Expr_Kind` (`nodes/parsenodes.h`) — kind of an `A_Expr` raw expression.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum A_Expr_Kind {
    /// normal operator
    #[default]
    AEXPR_OP = 0,
    /// scalar op ANY (array)
    AEXPR_OP_ANY = 1,
    /// scalar op ALL (array)
    AEXPR_OP_ALL = 2,
    /// IS DISTINCT FROM - name must be "="
    AEXPR_DISTINCT = 3,
    /// IS NOT DISTINCT FROM - name must be "="
    AEXPR_NOT_DISTINCT = 4,
    /// NULLIF - name must be "="
    AEXPR_NULLIF = 5,
    /// [NOT] IN - name must be "=" or "<>"
    AEXPR_IN = 6,
    /// [NOT] LIKE - name must be "~~" or "!~~"
    AEXPR_LIKE = 7,
    /// [NOT] ILIKE - name must be "~~*" or "!~~*"
    AEXPR_ILIKE = 8,
    /// [NOT] SIMILAR - name must be "~" or "!~"
    AEXPR_SIMILAR = 9,
    /// name must be "BETWEEN"
    AEXPR_BETWEEN = 10,
    /// name must be "NOT BETWEEN"
    AEXPR_NOT_BETWEEN = 11,
    /// name must be "BETWEEN SYMMETRIC"
    AEXPR_BETWEEN_SYM = 12,
    /// name must be "NOT BETWEEN SYMMETRIC"
    AEXPR_NOT_BETWEEN_SYM = 13,
}
pub use A_Expr_Kind::{
    AEXPR_BETWEEN, AEXPR_BETWEEN_SYM, AEXPR_DISTINCT, AEXPR_ILIKE, AEXPR_IN, AEXPR_LIKE,
    AEXPR_NOT_BETWEEN, AEXPR_NOT_BETWEEN_SYM, AEXPR_NOT_DISTINCT, AEXPR_NULLIF, AEXPR_OP,
    AEXPR_OP_ALL, AEXPR_OP_ANY, AEXPR_SIMILAR,
};

// ---------------------------------------------------------------------------
// Helpers for the uniform owned-tree `copyObject` shape (clone_in). The owned
// model re-homes every allocation onto a TARGET `mcx`; copy is fallible.
// ---------------------------------------------------------------------------

/// Deep-copy an `Option<NodePtr>` (`Node *` field) into `mcx`.
pub(crate) fn copy_opt_node<'b>(
    n: &Option<NodePtr<'_>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<NodePtr<'b>>> {
    match n {
        Some(n) => Ok(Some(mcx::alloc_in(mcx, n.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// Deep-copy a `PgVec<NodePtr>` (`List *` of nodes) into `mcx`.
pub(crate) fn copy_node_vec<'b>(
    v: &PgVec<'_, NodePtr<'_>>,
    mcx: Mcx<'b>,
) -> PgResult<PgVec<'b, NodePtr<'b>>> {
    let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
    for n in v.iter() {
        out.push(mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
    }
    Ok(out)
}

/// Deep-copy an `Option<PgString>` (`char *`) into `mcx`.
pub(crate) fn copy_opt_str<'b>(s: &Option<PgString<'_>>, mcx: Mcx<'b>) -> PgResult<Option<PgString<'b>>> {
    match s {
        Some(s) => Ok(Some(s.clone_in(mcx)?)),
        None => Ok(None),
    }
}

/// Deep-copy a `PgVec<Oid>`/`PgVec<i32>` scalar list into `mcx`.
fn copy_scalar_vec<'b, T: Copy>(v: &PgVec<'_, T>, mcx: Mcx<'b>) -> PgResult<PgVec<'b, T>> {
    let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
    for x in v.iter() {
        out.push(*x);
    }
    Ok(out)
}

// ===========================================================================
// Supporting data structures for Parse Trees (nodes/parsenodes.h)
// ===========================================================================

/// `Alias` (`nodes/primnodes.h`) — alias for a range variable.
#[derive(Debug)]
pub struct Alias<'mcx> {
    /// `char *aliasname` — aliased rel name (never qualified).
    pub aliasname: Option<PgString<'mcx>>,
    /// `List *colnames` — optional list of column aliases (String nodes).
    pub colnames: PgVec<'mcx, NodePtr<'mcx>>,
}

impl Alias<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `Alias`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Alias<'b>> {
        Ok(Alias {
            aliasname: copy_opt_str(&self.aliasname, mcx)?,
            colnames: copy_node_vec(&self.colnames, mcx)?,
        })
    }
}

/// `RangeVar` (`nodes/primnodes.h`) — range variable, used in FROM clauses and
/// to name relations in utility statements.
#[derive(Debug)]
pub struct RangeVar<'mcx> {
    /// `char *catalogname` — the catalog (database) name, or `None`.
    pub catalogname: Option<PgString<'mcx>>,
    /// `char *schemaname` — the schema name, or `None`.
    pub schemaname: Option<PgString<'mcx>>,
    /// `char *relname` — the relation/sequence name.
    pub relname: Option<PgString<'mcx>>,
    /// `bool inh` — expand rel by inheritance? recursively act on children?
    pub inh: bool,
    /// `char relpersistence` — see `RELPERSISTENCE_*`.
    pub relpersistence: i8,
    /// `Alias *alias` — table alias & optional column aliases.
    pub alias: Option<PgBox<'mcx, Alias<'mcx>>>,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: i32,
}

impl RangeVar<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `RangeVar`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RangeVar<'b>> {
        Ok(RangeVar {
            catalogname: copy_opt_str(&self.catalogname, mcx)?,
            schemaname: copy_opt_str(&self.schemaname, mcx)?,
            relname: copy_opt_str(&self.relname, mcx)?,
            inh: self.inh,
            relpersistence: self.relpersistence,
            alias: match &self.alias {
                Some(a) => Some(mcx::alloc_in(mcx, a.clone_in(mcx)?)?),
                None => None,
            },
            location: self.location,
        })
    }
}

/// `RangeTblRef` (`nodes/parsenodes.h`) — a reference to an `rtable` entry by
/// index, appearing in `jointree`/`setOperations`.
#[derive(Clone, Copy, Debug, Default)]
pub struct RangeTblRef {
    /// `int rtindex` — index into the query's range table.
    pub rtindex: i32,
}

impl RangeTblRef {
    /// Deep copy (scalar; C: `copyObject` over `RangeTblRef`).
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<RangeTblRef> {
        Ok(*self)
    }
}

/// `JoinExpr` (`nodes/primnodes.h`) — for SQL JOIN expressions.
#[derive(Debug)]
pub struct JoinExpr<'mcx> {
    /// `JoinType jointype` — type of join.
    pub jointype: JoinType,
    /// `bool isNatural` — natural join?
    pub isNatural: bool,
    /// `Node *larg` — left subtree.
    pub larg: Option<NodePtr<'mcx>>,
    /// `Node *rarg` — right subtree.
    pub rarg: Option<NodePtr<'mcx>>,
    /// `List *usingClause` — USING clause, if any (list of String).
    pub usingClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Alias *join_using_alias` — alias attached to USING clause, if any.
    pub join_using_alias: Option<PgBox<'mcx, Alias<'mcx>>>,
    /// `Node *quals` — qualifiers on join, if any.
    pub quals: Option<NodePtr<'mcx>>,
    /// `Alias *alias` — user-written alias clause, if any.
    pub alias: Option<PgBox<'mcx, Alias<'mcx>>>,
    /// `int rtindex` — RT index assigned for join, or 0.
    pub rtindex: i32,
}

impl JoinExpr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `JoinExpr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<JoinExpr<'b>> {
        Ok(JoinExpr {
            jointype: self.jointype,
            isNatural: self.isNatural,
            larg: copy_opt_node(&self.larg, mcx)?,
            rarg: copy_opt_node(&self.rarg, mcx)?,
            usingClause: copy_node_vec(&self.usingClause, mcx)?,
            join_using_alias: match &self.join_using_alias {
                Some(a) => Some(mcx::alloc_in(mcx, a.clone_in(mcx)?)?),
                None => None,
            },
            quals: copy_opt_node(&self.quals, mcx)?,
            alias: match &self.alias {
                Some(a) => Some(mcx::alloc_in(mcx, a.clone_in(mcx)?)?),
                None => None,
            },
            rtindex: self.rtindex,
        })
    }
}

/// `FromExpr` (`nodes/primnodes.h`) — a FROM ... WHERE ... construct.
#[derive(Debug)]
pub struct FromExpr<'mcx> {
    /// `List *fromlist` — list of join subtrees.
    pub fromlist: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *quals` — qualifiers on join, if any.
    pub quals: Option<NodePtr<'mcx>>,
}

impl FromExpr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `FromExpr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<FromExpr<'b>> {
        Ok(FromExpr {
            fromlist: copy_node_vec(&self.fromlist, mcx)?,
            quals: copy_opt_node(&self.quals, mcx)?,
        })
    }
}

/// `OnConflictExpr` (`nodes/primnodes.h`) — a transformed ON CONFLICT DO ...
/// expression (the post-analysis form; the raw form is [`OnConflictClause`]).
#[derive(Debug)]
pub struct OnConflictExpr<'mcx> {
    /// `OnConflictAction action` — DO NOTHING or UPDATE?
    pub action: OnConflictAction,
    /// `List *arbiterElems` — unique index arbiter list (of InferenceElem's).
    pub arbiterElems: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *arbiterWhere` — unique index arbiter WHERE clause.
    pub arbiterWhere: Option<NodePtr<'mcx>>,
    /// `Oid constraint` — `pg_constraint` OID for arbiter.
    pub constraint: Oid,
    /// `List *onConflictSet` — list of ON CONFLICT SET TargetEntrys.
    pub onConflictSet: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *onConflictWhere` — qualifiers to restrict UPDATE to.
    pub onConflictWhere: Option<NodePtr<'mcx>>,
    /// `int exclRelIndex` — RT index of 'excluded' relation.
    pub exclRelIndex: i32,
    /// `List *exclRelTlist` — tlist of the EXCLUDED pseudo relation.
    pub exclRelTlist: PgVec<'mcx, NodePtr<'mcx>>,
}

impl OnConflictExpr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `OnConflictExpr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<OnConflictExpr<'b>> {
        Ok(OnConflictExpr {
            action: self.action,
            arbiterElems: copy_node_vec(&self.arbiterElems, mcx)?,
            arbiterWhere: copy_opt_node(&self.arbiterWhere, mcx)?,
            constraint: self.constraint,
            onConflictSet: copy_node_vec(&self.onConflictSet, mcx)?,
            onConflictWhere: copy_opt_node(&self.onConflictWhere, mcx)?,
            exclRelIndex: self.exclRelIndex,
            exclRelTlist: copy_node_vec(&self.exclRelTlist, mcx)?,
        })
    }
}

/// `MergeAction` (`nodes/primnodes.h`) — a transformed WHEN clause of MERGE.
///
/// NOTE: the executor's `MergeActionState`-paired runtime `MergeAction` lives in
/// [`crate::modifytable`]; this is the parse-tree node. They mirror the same C
/// struct but the executor carries `ExprState`s the parser doesn't.
#[derive(Debug)]
pub struct MergeAction<'mcx> {
    /// `MergeMatchKind matchKind`.
    pub matchKind: crate::modifytable::MergeMatchKind,
    /// `CmdType commandType`.
    pub commandType: CmdType,
    /// `OverridingKind override`.
    pub r#override: crate::modifytable::OverridingKind,
    /// `Node *qual` — transformed WHEN conditions.
    pub qual: Option<NodePtr<'mcx>>,
    /// `List *targetList` — the target list (of TargetEntry).
    pub targetList: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *updateColnos` — target attribute numbers of an UPDATE.
    pub updateColnos: PgVec<'mcx, i32>,
}

impl MergeAction<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `MergeAction`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<MergeAction<'b>> {
        Ok(MergeAction {
            matchKind: self.matchKind,
            commandType: self.commandType,
            r#override: self.r#override,
            qual: copy_opt_node(&self.qual, mcx)?,
            targetList: copy_node_vec(&self.targetList, mcx)?,
            updateColnos: copy_scalar_vec(&self.updateColnos, mcx)?,
        })
    }
}

/// `RangeTblFunction` (`nodes/parsenodes.h`) — one function in an `RTE_FUNCTION`
/// RTE's `functions` list.
#[derive(Debug)]
pub struct RangeTblFunction<'mcx> {
    /// `Node *funcexpr` — expression tree for func call.
    pub funcexpr: Option<NodePtr<'mcx>>,
    /// `int funccolcount` — number of columns it contributes to RTE.
    pub funccolcount: i32,
    /// `List *funccolnames` — column names (list of String).
    pub funccolnames: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *funccoltypes` — OID list of column type OIDs.
    pub funccoltypes: PgVec<'mcx, Oid>,
    /// `List *funccoltypmods` — integer list of column typmods.
    pub funccoltypmods: PgVec<'mcx, i32>,
    /// `List *funccolcollations` — OID list of column collation OIDs.
    pub funccolcollations: PgVec<'mcx, Oid>,
    /// `Bitmapset *funcparams` — PARAM_EXEC Param IDs affecting this func (set
    /// during planning).
    pub funcparams: Option<PgBox<'mcx, crate::bitmapset::Bitmapset<'mcx>>>,
}

impl RangeTblFunction<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `RangeTblFunction`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RangeTblFunction<'b>> {
        Ok(RangeTblFunction {
            funcexpr: copy_opt_node(&self.funcexpr, mcx)?,
            funccolcount: self.funccolcount,
            funccolnames: copy_node_vec(&self.funccolnames, mcx)?,
            funccoltypes: copy_scalar_vec(&self.funccoltypes, mcx)?,
            funccoltypmods: copy_scalar_vec(&self.funccoltypmods, mcx)?,
            funccolcollations: copy_scalar_vec(&self.funccolcollations, mcx)?,
            funcparams: match &self.funcparams {
                Some(b) => Some(mcx::alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
}

/// `WithCheckOption` (`nodes/parsenodes.h`) — WITH CHECK OPTION / RLS check.
#[derive(Debug)]
pub struct WithCheckOption<'mcx> {
    /// `WCOKind kind` — kind of WCO.
    pub kind: WCOKind,
    /// `char *relname` — name of relation that specified the WCO.
    pub relname: Option<PgString<'mcx>>,
    /// `char *polname` — name of RLS policy being checked.
    pub polname: Option<PgString<'mcx>>,
    /// `Node *qual` — constraint qual to check.
    pub qual: Option<NodePtr<'mcx>>,
    /// `bool cascaded` — true for a cascaded WCO on a view.
    pub cascaded: bool,
}

impl WithCheckOption<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `WithCheckOption`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<WithCheckOption<'b>> {
        Ok(WithCheckOption {
            kind: self.kind,
            relname: copy_opt_str(&self.relname, mcx)?,
            polname: copy_opt_str(&self.polname, mcx)?,
            qual: copy_opt_node(&self.qual, mcx)?,
            cascaded: self.cascaded,
        })
    }
}

/// `SortGroupClause` (`nodes/parsenodes.h`) — ORDER BY/GROUP BY/PARTITION BY/
/// DISTINCT [ON] item.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SortGroupClause {
    /// `Index tleSortGroupRef` — reference into targetlist.
    pub tleSortGroupRef: Index,
    /// `Oid eqop` — the equality operator ('=' op).
    pub eqop: Oid,
    /// `Oid sortop` — the ordering operator ('<' op), or 0.
    pub sortop: Oid,
    /// `bool reverse_sort` — is sortop a "greater than" operator?
    pub reverse_sort: bool,
    /// `bool nulls_first` — do NULLs come before normal values?
    pub nulls_first: bool,
    /// `bool hashable` — can eqop be implemented by hashing?
    pub hashable: bool,
}

impl SortGroupClause {
    /// Deep copy (scalar; C: `copyObject` over `SortGroupClause`).
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<SortGroupClause> {
        Ok(*self)
    }
}

/// `GroupingSet` (`nodes/parsenodes.h`) — CUBE/ROLLUP/GROUPING SETS clause.
#[derive(Debug)]
pub struct GroupingSet<'mcx> {
    /// `GroupingSetKind kind`.
    pub kind: GroupingSetKind,
    /// `List *content`.
    pub content: PgVec<'mcx, NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl GroupingSet<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `GroupingSet`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<GroupingSet<'b>> {
        Ok(GroupingSet {
            kind: self.kind,
            content: copy_node_vec(&self.content, mcx)?,
            location: self.location,
        })
    }
}

/// `WindowClause` (`nodes/parsenodes.h`) — transformed WINDOW/OVER clause.
#[derive(Debug)]
pub struct WindowClause<'mcx> {
    /// `char *name` — window name (NULL in an OVER clause).
    pub name: Option<PgString<'mcx>>,
    /// `char *refname` — referenced window name, if any.
    pub refname: Option<PgString<'mcx>>,
    /// `List *partitionClause` — PARTITION BY list (of SortGroupClause).
    pub partitionClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *orderClause` — ORDER BY list (of SortGroupClause).
    pub orderClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `int frameOptions` — frame_clause options (see `WindowDef`).
    pub frameOptions: i32,
    /// `Node *startOffset` — expression for starting bound, if any.
    pub startOffset: Option<NodePtr<'mcx>>,
    /// `Node *endOffset` — expression for ending bound, if any.
    pub endOffset: Option<NodePtr<'mcx>>,
    /// `Oid startInRangeFunc` — in_range function for startOffset.
    pub startInRangeFunc: Oid,
    /// `Oid endInRangeFunc` — in_range function for endOffset.
    pub endInRangeFunc: Oid,
    /// `Oid inRangeColl` — collation for in_range tests.
    pub inRangeColl: Oid,
    /// `bool inRangeAsc` — use ASC sort order for in_range tests?
    pub inRangeAsc: bool,
    /// `bool inRangeNullsFirst` — nulls sort first for in_range tests?
    pub inRangeNullsFirst: bool,
    /// `Index winref` — ID referenced by window functions.
    pub winref: Index,
    /// `bool copiedOrder` — did we copy orderClause from refname?
    pub copiedOrder: bool,
}

impl WindowClause<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `WindowClause`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<WindowClause<'b>> {
        Ok(WindowClause {
            name: copy_opt_str(&self.name, mcx)?,
            refname: copy_opt_str(&self.refname, mcx)?,
            partitionClause: copy_node_vec(&self.partitionClause, mcx)?,
            orderClause: copy_node_vec(&self.orderClause, mcx)?,
            frameOptions: self.frameOptions,
            startOffset: copy_opt_node(&self.startOffset, mcx)?,
            endOffset: copy_opt_node(&self.endOffset, mcx)?,
            startInRangeFunc: self.startInRangeFunc,
            endInRangeFunc: self.endInRangeFunc,
            inRangeColl: self.inRangeColl,
            inRangeAsc: self.inRangeAsc,
            inRangeNullsFirst: self.inRangeNullsFirst,
            winref: self.winref,
            copiedOrder: self.copiedOrder,
        })
    }
}

/// `RowMarkClause` (`nodes/parsenodes.h`) — FOR [KEY] UPDATE/SHARE parser output.
#[derive(Clone, Copy, Debug, Default)]
pub struct RowMarkClause {
    /// `Index rti` — range table index of target relation.
    pub rti: Index,
    /// `LockClauseStrength strength`.
    pub strength: LockClauseStrength,
    /// `LockWaitPolicy waitPolicy` — NOWAIT and SKIP LOCKED.
    pub waitPolicy: LockWaitPolicy,
    /// `bool pushedDown` — pushed down from higher query level?
    pub pushedDown: bool,
}

impl RowMarkClause {
    /// Deep copy (scalar; C: `copyObject` over `RowMarkClause`).
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<RowMarkClause> {
        Ok(*self)
    }
}

/// `CommonTableExpr` (`nodes/parsenodes.h`) — a WITH list element.
#[derive(Debug)]
pub struct CommonTableExpr<'mcx> {
    /// `char *ctename` — query name (never qualified).
    pub ctename: Option<PgString<'mcx>>,
    /// `List *aliascolnames` — optional list of column names.
    pub aliascolnames: PgVec<'mcx, NodePtr<'mcx>>,
    /// `CTEMaterialize ctematerialized` — is this an optimization fence?
    pub ctematerialized: CTEMaterialize,
    /// `Node *ctequery` — the CTE's subquery (SelectStmt/... before analysis,
    /// Query afterwards).
    pub ctequery: Option<NodePtr<'mcx>>,
    /// `CTESearchClause *search_clause`.
    pub search_clause: Option<PgBox<'mcx, CTESearchClause<'mcx>>>,
    /// `CTECycleClause *cycle_clause`.
    pub cycle_clause: Option<PgBox<'mcx, CTECycleClause<'mcx>>>,
    /// `ParseLoc location`.
    pub location: i32,
    /// `bool cterecursive` — is this CTE actually recursive? (set in analysis)
    pub cterecursive: bool,
    /// `int cterefcount` — number of RTEs referencing this CTE.
    pub cterefcount: i32,
    /// `List *ctecolnames` — list of output column names.
    pub ctecolnames: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *ctecoltypes` — OID list of output column type OIDs.
    pub ctecoltypes: PgVec<'mcx, Oid>,
    /// `List *ctecoltypmods` — integer list of output column typmods.
    pub ctecoltypmods: PgVec<'mcx, i32>,
    /// `List *ctecolcollations` — OID list of column collation OIDs.
    pub ctecolcollations: PgVec<'mcx, Oid>,
}

impl CommonTableExpr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `CommonTableExpr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CommonTableExpr<'b>> {
        Ok(CommonTableExpr {
            ctename: copy_opt_str(&self.ctename, mcx)?,
            aliascolnames: copy_node_vec(&self.aliascolnames, mcx)?,
            ctematerialized: self.ctematerialized,
            ctequery: copy_opt_node(&self.ctequery, mcx)?,
            search_clause: match &self.search_clause {
                Some(s) => Some(mcx::alloc_in(mcx, s.clone_in(mcx)?)?),
                None => None,
            },
            cycle_clause: match &self.cycle_clause {
                Some(c) => Some(mcx::alloc_in(mcx, c.clone_in(mcx)?)?),
                None => None,
            },
            location: self.location,
            cterecursive: self.cterecursive,
            cterefcount: self.cterefcount,
            ctecolnames: copy_node_vec(&self.ctecolnames, mcx)?,
            ctecoltypes: copy_scalar_vec(&self.ctecoltypes, mcx)?,
            ctecoltypmods: copy_scalar_vec(&self.ctecoltypmods, mcx)?,
            ctecolcollations: copy_scalar_vec(&self.ctecolcollations, mcx)?,
        })
    }
}

/// `CTESearchClause` (`nodes/parsenodes.h`).
#[derive(Debug)]
pub struct CTESearchClause<'mcx> {
    /// `List *search_col_list`.
    pub search_col_list: PgVec<'mcx, NodePtr<'mcx>>,
    /// `bool search_breadth_first`.
    pub search_breadth_first: bool,
    /// `char *search_seq_column`.
    pub search_seq_column: Option<PgString<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl CTESearchClause<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `CTESearchClause`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CTESearchClause<'b>> {
        Ok(CTESearchClause {
            search_col_list: copy_node_vec(&self.search_col_list, mcx)?,
            search_breadth_first: self.search_breadth_first,
            search_seq_column: copy_opt_str(&self.search_seq_column, mcx)?,
            location: self.location,
        })
    }
}

/// `CTECycleClause` (`nodes/parsenodes.h`).
#[derive(Debug)]
pub struct CTECycleClause<'mcx> {
    /// `List *cycle_col_list`.
    pub cycle_col_list: PgVec<'mcx, NodePtr<'mcx>>,
    /// `char *cycle_mark_column`.
    pub cycle_mark_column: Option<PgString<'mcx>>,
    /// `Node *cycle_mark_value`.
    pub cycle_mark_value: Option<NodePtr<'mcx>>,
    /// `Node *cycle_mark_default`.
    pub cycle_mark_default: Option<NodePtr<'mcx>>,
    /// `char *cycle_path_column`.
    pub cycle_path_column: Option<PgString<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
    /// `Oid cycle_mark_type` — common type of _value and _default.
    pub cycle_mark_type: Oid,
    /// `int cycle_mark_typmod`.
    pub cycle_mark_typmod: i32,
    /// `Oid cycle_mark_collation`.
    pub cycle_mark_collation: Oid,
    /// `Oid cycle_mark_neop` — `<>` operator for type.
    pub cycle_mark_neop: Oid,
}

impl CTECycleClause<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `CTECycleClause`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CTECycleClause<'b>> {
        Ok(CTECycleClause {
            cycle_col_list: copy_node_vec(&self.cycle_col_list, mcx)?,
            cycle_mark_column: copy_opt_str(&self.cycle_mark_column, mcx)?,
            cycle_mark_value: copy_opt_node(&self.cycle_mark_value, mcx)?,
            cycle_mark_default: copy_opt_node(&self.cycle_mark_default, mcx)?,
            cycle_path_column: copy_opt_str(&self.cycle_path_column, mcx)?,
            location: self.location,
            cycle_mark_type: self.cycle_mark_type,
            cycle_mark_typmod: self.cycle_mark_typmod,
            cycle_mark_collation: self.cycle_mark_collation,
            cycle_mark_neop: self.cycle_mark_neop,
        })
    }
}

/// `SetOperationStmt` (`nodes/parsenodes.h`) — set-operation node for
/// post-analysis query trees.
#[derive(Debug)]
pub struct SetOperationStmt<'mcx> {
    /// `SetOperation op` — type of set op.
    pub op: SetOperation,
    /// `bool all` — ALL specified?
    pub all: bool,
    /// `Node *larg` — left child.
    pub larg: Option<NodePtr<'mcx>>,
    /// `Node *rarg` — right child.
    pub rarg: Option<NodePtr<'mcx>>,
    /// `List *colTypes` — OID list of output column type OIDs.
    pub colTypes: PgVec<'mcx, Oid>,
    /// `List *colTypmods` — integer list of output column typmods.
    pub colTypmods: PgVec<'mcx, i32>,
    /// `List *colCollations` — OID list of output column collation OIDs.
    pub colCollations: PgVec<'mcx, Oid>,
    /// `List *groupClauses` — a list of SortGroupClause's.
    pub groupClauses: PgVec<'mcx, NodePtr<'mcx>>,
}

impl SetOperationStmt<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `SetOperationStmt`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<SetOperationStmt<'b>> {
        Ok(SetOperationStmt {
            op: self.op,
            all: self.all,
            larg: copy_opt_node(&self.larg, mcx)?,
            rarg: copy_opt_node(&self.rarg, mcx)?,
            colTypes: copy_scalar_vec(&self.colTypes, mcx)?,
            colTypmods: copy_scalar_vec(&self.colTypmods, mcx)?,
            colCollations: copy_scalar_vec(&self.colCollations, mcx)?,
            groupClauses: copy_node_vec(&self.groupClauses, mcx)?,
        })
    }
}

// ===========================================================================
// Raw-grammar INPUT nodes (nodes/parsenodes.h) — the trees gram.y emits and
// analyze.c / parse_clause.c / parse_expr.c consume.
// ===========================================================================

/// `TypeName` (`nodes/parsenodes.h`) — specifies a type in definitions.
///
/// NOTE: a separate consumer-trimmed `TypeName` (the PREPARE driver's) lives in
/// [`types_opclass`]; this is the full raw-grammar node.
#[derive(Debug)]
pub struct TypeName<'mcx> {
    /// `List *names` — qualified name (list of String nodes).
    pub names: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Oid typeOid` — type identified by OID.
    pub typeOid: Oid,
    /// `bool setof` — is a set?
    pub setof: bool,
    /// `bool pct_type` — %TYPE specified?
    pub pct_type: bool,
    /// `List *typmods` — type modifier expression(s).
    pub typmods: PgVec<'mcx, NodePtr<'mcx>>,
    /// `int32 typemod` — prespecified type modifier.
    pub typemod: i32,
    /// `List *arrayBounds` — array bounds.
    pub arrayBounds: PgVec<'mcx, NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl TypeName<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `TypeName`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<TypeName<'b>> {
        Ok(TypeName {
            names: copy_node_vec(&self.names, mcx)?,
            typeOid: self.typeOid,
            setof: self.setof,
            pct_type: self.pct_type,
            typmods: copy_node_vec(&self.typmods, mcx)?,
            typemod: self.typemod,
            arrayBounds: copy_node_vec(&self.arrayBounds, mcx)?,
            location: self.location,
        })
    }
}

/// `ColumnRef` (`nodes/parsenodes.h`) — a reference to a column or whole tuple.
#[derive(Debug)]
pub struct ColumnRef<'mcx> {
    /// `List *fields` — field names (String nodes) or `A_Star`.
    pub fields: PgVec<'mcx, NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl ColumnRef<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `ColumnRef`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ColumnRef<'b>> {
        Ok(ColumnRef {
            fields: copy_node_vec(&self.fields, mcx)?,
            location: self.location,
        })
    }
}

/// `ParamRef` (`nodes/parsenodes.h`) — a `$n` parameter reference.
#[derive(Clone, Copy, Debug, Default)]
pub struct ParamRef {
    /// `int number` — the number of the parameter.
    pub number: i32,
    /// `ParseLoc location`.
    pub location: i32,
}

impl ParamRef {
    /// Deep copy (scalar; C: `copyObject` over `ParamRef`).
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<ParamRef> {
        Ok(*self)
    }
}

/// `A_Expr` (`nodes/parsenodes.h`) — infix/prefix/postfix expression.
#[derive(Debug)]
pub struct A_Expr<'mcx> {
    /// `A_Expr_Kind kind`.
    pub kind: A_Expr_Kind,
    /// `List *name` — possibly-qualified name of operator.
    pub name: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *lexpr` — left argument, or NULL if none.
    pub lexpr: Option<NodePtr<'mcx>>,
    /// `Node *rexpr` — right argument, or NULL if none.
    pub rexpr: Option<NodePtr<'mcx>>,
    /// `ParseLoc rexpr_list_start`.
    pub rexpr_list_start: i32,
    /// `ParseLoc rexpr_list_end`.
    pub rexpr_list_end: i32,
    /// `ParseLoc location`.
    pub location: i32,
}

impl A_Expr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `A_Expr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<A_Expr<'b>> {
        Ok(A_Expr {
            kind: self.kind,
            name: copy_node_vec(&self.name, mcx)?,
            lexpr: copy_opt_node(&self.lexpr, mcx)?,
            rexpr: copy_opt_node(&self.rexpr, mcx)?,
            rexpr_list_start: self.rexpr_list_start,
            rexpr_list_end: self.rexpr_list_end,
            location: self.location,
        })
    }
}

/// `A_Const` (`nodes/parsenodes.h`) — a literal constant. `val` rides the owned
/// value node through [`Node`](crate::nodes::Node)
/// (`Integer`/`Float`/`Boolean`/`String`/`BitString`);
/// `isnull` selects SQL NULL (then `val` is absent).
#[derive(Debug)]
pub struct A_Const<'mcx> {
    /// `union ValUnion val` — the literal value node (absent if `isnull`).
    pub val: Option<NodePtr<'mcx>>,
    /// `bool isnull` — SQL NULL constant.
    pub isnull: bool,
    /// `ParseLoc location`.
    pub location: i32,
}

impl A_Const<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `A_Const`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<A_Const<'b>> {
        Ok(A_Const {
            val: copy_opt_node(&self.val, mcx)?,
            isnull: self.isnull,
            location: self.location,
        })
    }
}

/// `TypeCast` (`nodes/parsenodes.h`) — a CAST expression.
#[derive(Debug)]
pub struct TypeCast<'mcx> {
    /// `Node *arg` — the expression being casted.
    pub arg: Option<NodePtr<'mcx>>,
    /// `TypeName *typeName` — the target type.
    pub typeName: Option<PgBox<'mcx, TypeName<'mcx>>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl TypeCast<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `TypeCast`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<TypeCast<'b>> {
        Ok(TypeCast {
            arg: copy_opt_node(&self.arg, mcx)?,
            typeName: match &self.typeName {
                Some(t) => Some(mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            location: self.location,
        })
    }
}

/// `CollateClause` (`nodes/parsenodes.h`) — a COLLATE expression.
#[derive(Debug)]
pub struct CollateClause<'mcx> {
    /// `Node *arg` — input expression.
    pub arg: Option<NodePtr<'mcx>>,
    /// `List *collname` — possibly-qualified collation name.
    pub collname: PgVec<'mcx, NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl CollateClause<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `CollateClause`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CollateClause<'b>> {
        Ok(CollateClause {
            arg: copy_opt_node(&self.arg, mcx)?,
            collname: copy_node_vec(&self.collname, mcx)?,
            location: self.location,
        })
    }
}

/// `FuncCall` (`nodes/parsenodes.h`) — a function or aggregate invocation.
#[derive(Debug)]
pub struct FuncCall<'mcx> {
    /// `List *funcname` — qualified name of function.
    pub funcname: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *args` — the arguments (list of exprs).
    pub args: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *agg_order` — ORDER BY (list of SortBy).
    pub agg_order: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *agg_filter` — FILTER clause, if any.
    pub agg_filter: Option<NodePtr<'mcx>>,
    /// `WindowDef *over` — OVER clause, if any.
    pub over: Option<PgBox<'mcx, WindowDef<'mcx>>>,
    /// `bool agg_within_group` — ORDER BY appeared in WITHIN GROUP.
    pub agg_within_group: bool,
    /// `bool agg_star` — argument was really '*'.
    pub agg_star: bool,
    /// `bool agg_distinct` — arguments were labeled DISTINCT.
    pub agg_distinct: bool,
    /// `bool func_variadic` — last argument was labeled VARIADIC.
    pub func_variadic: bool,
    /// `CoercionForm funcformat` — how to display this node.
    pub funcformat: CoercionForm,
    /// `ParseLoc location`.
    pub location: i32,
}

impl FuncCall<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `FuncCall`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<FuncCall<'b>> {
        Ok(FuncCall {
            funcname: copy_node_vec(&self.funcname, mcx)?,
            args: copy_node_vec(&self.args, mcx)?,
            agg_order: copy_node_vec(&self.agg_order, mcx)?,
            agg_filter: copy_opt_node(&self.agg_filter, mcx)?,
            over: match &self.over {
                Some(w) => Some(mcx::alloc_in(mcx, w.clone_in(mcx)?)?),
                None => None,
            },
            agg_within_group: self.agg_within_group,
            agg_star: self.agg_star,
            agg_distinct: self.agg_distinct,
            func_variadic: self.func_variadic,
            funcformat: self.funcformat,
            location: self.location,
        })
    }
}

/// `A_Star` (`nodes/parsenodes.h`) — '*' representing all columns.
#[derive(Clone, Copy, Debug, Default)]
pub struct A_Star;

impl A_Star {
    /// Deep copy (empty; C: `copyObject` over `A_Star`).
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<A_Star> {
        Ok(A_Star)
    }
}

/// `A_Indices` (`nodes/parsenodes.h`) — array subscript or slice bounds.
#[derive(Debug)]
pub struct A_Indices<'mcx> {
    /// `bool is_slice` — true if slice (i.e., colon present).
    pub is_slice: bool,
    /// `Node *lidx` — slice lower bound, if any.
    pub lidx: Option<NodePtr<'mcx>>,
    /// `Node *uidx` — subscript, or slice upper bound if any.
    pub uidx: Option<NodePtr<'mcx>>,
}

impl A_Indices<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `A_Indices`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<A_Indices<'b>> {
        Ok(A_Indices {
            is_slice: self.is_slice,
            lidx: copy_opt_node(&self.lidx, mcx)?,
            uidx: copy_opt_node(&self.uidx, mcx)?,
        })
    }
}

/// `A_Indirection` (`nodes/parsenodes.h`) — select a field and/or array element.
#[derive(Debug)]
pub struct A_Indirection<'mcx> {
    /// `Node *arg` — the thing being selected from.
    pub arg: Option<NodePtr<'mcx>>,
    /// `List *indirection` — subscripts and/or field names and/or '*'.
    pub indirection: PgVec<'mcx, NodePtr<'mcx>>,
}

impl A_Indirection<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `A_Indirection`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<A_Indirection<'b>> {
        Ok(A_Indirection {
            arg: copy_opt_node(&self.arg, mcx)?,
            indirection: copy_node_vec(&self.indirection, mcx)?,
        })
    }
}

/// `A_ArrayExpr` (`nodes/parsenodes.h`) — an `ARRAY[]` construct.
#[derive(Debug)]
pub struct A_ArrayExpr<'mcx> {
    /// `List *elements` — array element expressions.
    pub elements: PgVec<'mcx, NodePtr<'mcx>>,
    /// `ParseLoc list_start`.
    pub list_start: i32,
    /// `ParseLoc list_end`.
    pub list_end: i32,
    /// `ParseLoc location`.
    pub location: i32,
}

impl A_ArrayExpr<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `A_ArrayExpr`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<A_ArrayExpr<'b>> {
        Ok(A_ArrayExpr {
            elements: copy_node_vec(&self.elements, mcx)?,
            list_start: self.list_start,
            list_end: self.list_end,
            location: self.location,
        })
    }
}

/// `ResTarget` (`nodes/parsenodes.h`) — result target in a pre-transformed
/// parse tree's target list.
#[derive(Debug)]
pub struct ResTarget<'mcx> {
    /// `char *name` — column name or NULL.
    pub name: Option<PgString<'mcx>>,
    /// `List *indirection` — subscripts, field names, and '*', or NIL.
    pub indirection: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *val` — the value expression to compute or assign.
    pub val: Option<NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl ResTarget<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `ResTarget`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ResTarget<'b>> {
        Ok(ResTarget {
            name: copy_opt_str(&self.name, mcx)?,
            indirection: copy_node_vec(&self.indirection, mcx)?,
            val: copy_opt_node(&self.val, mcx)?,
            location: self.location,
        })
    }
}

/// `MultiAssignRef` (`nodes/parsenodes.h`) — element of a row source expr for
/// UPDATE.
#[derive(Debug)]
pub struct MultiAssignRef<'mcx> {
    /// `Node *source` — the row-valued expression.
    pub source: Option<NodePtr<'mcx>>,
    /// `int colno` — column number for this target (1..n).
    pub colno: i32,
    /// `int ncolumns` — number of targets in the construct.
    pub ncolumns: i32,
}

impl MultiAssignRef<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `MultiAssignRef`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<MultiAssignRef<'b>> {
        Ok(MultiAssignRef {
            source: copy_opt_node(&self.source, mcx)?,
            colno: self.colno,
            ncolumns: self.ncolumns,
        })
    }
}

/// `SortBy` (`nodes/parsenodes.h`) — for the ORDER BY clause (raw).
#[derive(Debug)]
pub struct SortBy<'mcx> {
    /// `Node *node` — expression to sort on.
    pub node: Option<NodePtr<'mcx>>,
    /// `SortByDir sortby_dir` — ASC/DESC/USING/default.
    pub sortby_dir: SortByDir,
    /// `SortByNulls sortby_nulls` — NULLS FIRST/LAST.
    pub sortby_nulls: SortByNulls,
    /// `List *useOp` — name of op to use, if SORTBY_USING.
    pub useOp: PgVec<'mcx, NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl SortBy<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `SortBy`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<SortBy<'b>> {
        Ok(SortBy {
            node: copy_opt_node(&self.node, mcx)?,
            sortby_dir: self.sortby_dir,
            sortby_nulls: self.sortby_nulls,
            useOp: copy_node_vec(&self.useOp, mcx)?,
            location: self.location,
        })
    }
}

/// `WindowDef` (`nodes/parsenodes.h`) — raw representation of WINDOW/OVER.
#[derive(Debug)]
pub struct WindowDef<'mcx> {
    /// `char *name` — window's own name.
    pub name: Option<PgString<'mcx>>,
    /// `char *refname` — referenced window name, if any.
    pub refname: Option<PgString<'mcx>>,
    /// `List *partitionClause` — PARTITION BY expression list.
    pub partitionClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *orderClause` — ORDER BY (list of SortBy).
    pub orderClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `int frameOptions` — frame_clause options.
    pub frameOptions: i32,
    /// `Node *startOffset` — expression for starting bound, if any.
    pub startOffset: Option<NodePtr<'mcx>>,
    /// `Node *endOffset` — expression for ending bound, if any.
    pub endOffset: Option<NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl WindowDef<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `WindowDef`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<WindowDef<'b>> {
        Ok(WindowDef {
            name: copy_opt_str(&self.name, mcx)?,
            refname: copy_opt_str(&self.refname, mcx)?,
            partitionClause: copy_node_vec(&self.partitionClause, mcx)?,
            orderClause: copy_node_vec(&self.orderClause, mcx)?,
            frameOptions: self.frameOptions,
            startOffset: copy_opt_node(&self.startOffset, mcx)?,
            endOffset: copy_opt_node(&self.endOffset, mcx)?,
            location: self.location,
        })
    }
}

/// `RangeSubselect` (`nodes/parsenodes.h`) — subquery in a FROM clause.
#[derive(Debug)]
pub struct RangeSubselect<'mcx> {
    /// `bool lateral` — does it have LATERAL prefix?
    pub lateral: bool,
    /// `Node *subquery` — the untransformed sub-select clause.
    pub subquery: Option<NodePtr<'mcx>>,
    /// `Alias *alias` — table alias & optional column aliases.
    pub alias: Option<PgBox<'mcx, Alias<'mcx>>>,
}

impl RangeSubselect<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `RangeSubselect`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RangeSubselect<'b>> {
        Ok(RangeSubselect {
            lateral: self.lateral,
            subquery: copy_opt_node(&self.subquery, mcx)?,
            alias: match &self.alias {
                Some(a) => Some(mcx::alloc_in(mcx, a.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
}

/// `RangeFunction` (`nodes/parsenodes.h`) — function call in a FROM clause.
#[derive(Debug)]
pub struct RangeFunction<'mcx> {
    /// `bool lateral` — does it have LATERAL prefix?
    pub lateral: bool,
    /// `bool ordinality` — does it have WITH ORDINALITY suffix?
    pub ordinality: bool,
    /// `bool is_rowsfrom` — is result of ROWS FROM() syntax?
    pub is_rowsfrom: bool,
    /// `List *functions` — per-function information (each a 2-elem sublist).
    pub functions: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Alias *alias` — table alias & optional column aliases.
    pub alias: Option<PgBox<'mcx, Alias<'mcx>>>,
    /// `List *coldeflist` — list of ColumnDef nodes describing RECORD result.
    pub coldeflist: PgVec<'mcx, NodePtr<'mcx>>,
}

impl RangeFunction<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `RangeFunction`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RangeFunction<'b>> {
        Ok(RangeFunction {
            lateral: self.lateral,
            ordinality: self.ordinality,
            is_rowsfrom: self.is_rowsfrom,
            functions: copy_node_vec(&self.functions, mcx)?,
            alias: match &self.alias {
                Some(a) => Some(mcx::alloc_in(mcx, a.clone_in(mcx)?)?),
                None => None,
            },
            coldeflist: copy_node_vec(&self.coldeflist, mcx)?,
        })
    }
}

/// `RangeTableSample` (`nodes/parsenodes.h`) — TABLESAMPLE in a raw FROM clause.
#[derive(Debug)]
pub struct RangeTableSample<'mcx> {
    /// `Node *relation` — relation to be sampled.
    pub relation: Option<NodePtr<'mcx>>,
    /// `List *method` — sampling method name (possibly qualified).
    pub method: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *args` — argument(s) for sampling method.
    pub args: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *repeatable` — REPEATABLE expression, or NULL if none.
    pub repeatable: Option<NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl RangeTableSample<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `RangeTableSample`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RangeTableSample<'b>> {
        Ok(RangeTableSample {
            relation: copy_opt_node(&self.relation, mcx)?,
            method: copy_node_vec(&self.method, mcx)?,
            args: copy_node_vec(&self.args, mcx)?,
            repeatable: copy_opt_node(&self.repeatable, mcx)?,
            location: self.location,
        })
    }
}

/// `ColumnDef` (`nodes/parsenodes.h`) — column definition (used in creates).
#[derive(Debug)]
pub struct ColumnDef<'mcx> {
    /// `char *colname` — name of column.
    pub colname: Option<PgString<'mcx>>,
    /// `TypeName *typeName` — type of column.
    pub typeName: Option<PgBox<'mcx, TypeName<'mcx>>>,
    /// `char *compression` — compression method for column.
    pub compression: Option<PgString<'mcx>>,
    /// `int16 inhcount` — number of times column is inherited.
    pub inhcount: i16,
    /// `bool is_local` — column has local (non-inherited) def'n.
    pub is_local: bool,
    /// `bool is_not_null` — NOT NULL constraint specified?
    pub is_not_null: bool,
    /// `bool is_from_type` — column definition came from table type.
    pub is_from_type: bool,
    /// `char storage` — attstorage setting, or 0 for default.
    pub storage: i8,
    /// `char *storage_name` — attstorage setting name or NULL for default.
    pub storage_name: Option<PgString<'mcx>>,
    /// `Node *raw_default` — default value (untransformed parse tree).
    pub raw_default: Option<NodePtr<'mcx>>,
    /// `Node *cooked_default` — default value (transformed expr tree).
    pub cooked_default: Option<NodePtr<'mcx>>,
    /// `char identity` — attidentity setting.
    pub identity: i8,
    /// `RangeVar *identitySequence` — identity sequence name for ADD COLUMN.
    pub identitySequence: Option<PgBox<'mcx, RangeVar<'mcx>>>,
    /// `char generated` — attgenerated setting.
    pub generated: i8,
    /// `CollateClause *collClause` — untransformed COLLATE spec, if any.
    pub collClause: Option<PgBox<'mcx, CollateClause<'mcx>>>,
    /// `Oid collOid` — collation OID (InvalidOid if not set).
    pub collOid: Oid,
    /// `List *constraints` — other constraints on column.
    pub constraints: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *fdwoptions` — per-column FDW options.
    pub fdwoptions: PgVec<'mcx, NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl ColumnDef<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `ColumnDef`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ColumnDef<'b>> {
        Ok(ColumnDef {
            colname: copy_opt_str(&self.colname, mcx)?,
            typeName: match &self.typeName {
                Some(t) => Some(mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
                None => None,
            },
            compression: copy_opt_str(&self.compression, mcx)?,
            inhcount: self.inhcount,
            is_local: self.is_local,
            is_not_null: self.is_not_null,
            is_from_type: self.is_from_type,
            storage: self.storage,
            storage_name: copy_opt_str(&self.storage_name, mcx)?,
            raw_default: copy_opt_node(&self.raw_default, mcx)?,
            cooked_default: copy_opt_node(&self.cooked_default, mcx)?,
            identity: self.identity,
            identitySequence: match &self.identitySequence {
                Some(r) => Some(mcx::alloc_in(mcx, r.clone_in(mcx)?)?),
                None => None,
            },
            generated: self.generated,
            collClause: match &self.collClause {
                Some(c) => Some(mcx::alloc_in(mcx, c.clone_in(mcx)?)?),
                None => None,
            },
            collOid: self.collOid,
            constraints: copy_node_vec(&self.constraints, mcx)?,
            fdwoptions: copy_node_vec(&self.fdwoptions, mcx)?,
            location: self.location,
        })
    }
}

/// `WithClause` (`nodes/parsenodes.h`) — a WITH clause (raw; does not propagate
/// into the `Query`).
#[derive(Debug)]
pub struct WithClause<'mcx> {
    /// `List *ctes` — list of CommonTableExprs.
    pub ctes: PgVec<'mcx, NodePtr<'mcx>>,
    /// `bool recursive` — WITH RECURSIVE?
    pub recursive: bool,
    /// `ParseLoc location`.
    pub location: i32,
}

impl WithClause<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `WithClause`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<WithClause<'b>> {
        Ok(WithClause {
            ctes: copy_node_vec(&self.ctes, mcx)?,
            recursive: self.recursive,
            location: self.location,
        })
    }
}

/// `InferClause` (`nodes/parsenodes.h`) — ON CONFLICT unique-index inference.
#[derive(Debug)]
pub struct InferClause<'mcx> {
    /// `List *indexElems` — IndexElems to infer unique index.
    pub indexElems: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *whereClause` — partial-index predicate qualification.
    pub whereClause: Option<NodePtr<'mcx>>,
    /// `char *conname` — constraint name, or NULL if unnamed.
    pub conname: Option<PgString<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl InferClause<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `InferClause`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<InferClause<'b>> {
        Ok(InferClause {
            indexElems: copy_node_vec(&self.indexElems, mcx)?,
            whereClause: copy_opt_node(&self.whereClause, mcx)?,
            conname: copy_opt_str(&self.conname, mcx)?,
            location: self.location,
        })
    }
}

/// `OnConflictClause` (`nodes/parsenodes.h`) — raw ON CONFLICT clause (does not
/// propagate into the `Query`; transformed into [`OnConflictExpr`]).
#[derive(Debug)]
pub struct OnConflictClause<'mcx> {
    /// `OnConflictAction action` — DO NOTHING or UPDATE?
    pub action: OnConflictAction,
    /// `InferClause *infer` — optional index inference clause.
    pub infer: Option<PgBox<'mcx, InferClause<'mcx>>>,
    /// `List *targetList` — the target list (of ResTarget).
    pub targetList: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *whereClause` — qualifications.
    pub whereClause: Option<NodePtr<'mcx>>,
    /// `ParseLoc location`.
    pub location: i32,
}

impl OnConflictClause<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `OnConflictClause`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<OnConflictClause<'b>> {
        Ok(OnConflictClause {
            action: self.action,
            infer: match &self.infer {
                Some(i) => Some(mcx::alloc_in(mcx, i.clone_in(mcx)?)?),
                None => None,
            },
            targetList: copy_node_vec(&self.targetList, mcx)?,
            whereClause: copy_opt_node(&self.whereClause, mcx)?,
            location: self.location,
        })
    }
}

/// `MergeWhenClause` (`nodes/parsenodes.h`) — raw WHEN clause of MERGE.
#[derive(Debug)]
pub struct MergeWhenClause<'mcx> {
    /// `MergeMatchKind matchKind`.
    pub matchKind: crate::modifytable::MergeMatchKind,
    /// `CmdType commandType` — INSERT/UPDATE/DELETE/DO NOTHING.
    pub commandType: CmdType,
    /// `OverridingKind override`.
    pub r#override: crate::modifytable::OverridingKind,
    /// `Node *condition` — WHEN conditions (raw parser).
    pub condition: Option<NodePtr<'mcx>>,
    /// `List *targetList` — INSERT/UPDATE targetlist.
    pub targetList: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *values` — VALUES to INSERT, or NULL.
    pub values: PgVec<'mcx, NodePtr<'mcx>>,
}

impl MergeWhenClause<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `MergeWhenClause`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<MergeWhenClause<'b>> {
        Ok(MergeWhenClause {
            matchKind: self.matchKind,
            commandType: self.commandType,
            r#override: self.r#override,
            condition: copy_opt_node(&self.condition, mcx)?,
            targetList: copy_node_vec(&self.targetList, mcx)?,
            values: copy_node_vec(&self.values, mcx)?,
        })
    }
}

/// `ReturningClause` (`nodes/parsenodes.h`) — raw RETURNING clause.
#[derive(Debug)]
pub struct ReturningClause<'mcx> {
    /// `List *options` — list of ReturningOption elements.
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *exprs` — list of expressions to return.
    pub exprs: PgVec<'mcx, NodePtr<'mcx>>,
}

impl ReturningClause<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `ReturningClause`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ReturningClause<'b>> {
        Ok(ReturningClause {
            options: copy_node_vec(&self.options, mcx)?,
            exprs: copy_node_vec(&self.exprs, mcx)?,
        })
    }
}

// ===========================================================================
// Raw statement nodes (nodes/parsenodes.h)
// ===========================================================================

/// `InsertStmt` (`nodes/parsenodes.h`) — the raw `INSERT` statement.
#[derive(Debug)]
pub struct InsertStmt<'mcx> {
    /// `RangeVar *relation` — relation to insert into.
    pub relation: Option<PgBox<'mcx, RangeVar<'mcx>>>,
    /// `List *cols` — optional: names of the target columns.
    pub cols: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *selectStmt` — the source SELECT/VALUES, or NULL.
    pub selectStmt: Option<NodePtr<'mcx>>,
    /// `OnConflictClause *onConflictClause`.
    pub onConflictClause: Option<PgBox<'mcx, OnConflictClause<'mcx>>>,
    /// `ReturningClause *returningClause`.
    pub returningClause: Option<PgBox<'mcx, ReturningClause<'mcx>>>,
    /// `WithClause *withClause`.
    pub withClause: Option<PgBox<'mcx, WithClause<'mcx>>>,
    /// `OverridingKind override`.
    pub r#override: crate::modifytable::OverridingKind,
}

impl InsertStmt<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `InsertStmt`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<InsertStmt<'b>> {
        Ok(InsertStmt {
            relation: match &self.relation {
                Some(r) => Some(mcx::alloc_in(mcx, r.clone_in(mcx)?)?),
                None => None,
            },
            cols: copy_node_vec(&self.cols, mcx)?,
            selectStmt: copy_opt_node(&self.selectStmt, mcx)?,
            onConflictClause: match &self.onConflictClause {
                Some(o) => Some(mcx::alloc_in(mcx, o.clone_in(mcx)?)?),
                None => None,
            },
            returningClause: match &self.returningClause {
                Some(r) => Some(mcx::alloc_in(mcx, r.clone_in(mcx)?)?),
                None => None,
            },
            withClause: match &self.withClause {
                Some(w) => Some(mcx::alloc_in(mcx, w.clone_in(mcx)?)?),
                None => None,
            },
            r#override: self.r#override,
        })
    }
}

/// `DeleteStmt` (`nodes/parsenodes.h`) — the raw `DELETE` statement.
#[derive(Debug)]
pub struct DeleteStmt<'mcx> {
    /// `RangeVar *relation` — relation to delete from.
    pub relation: Option<PgBox<'mcx, RangeVar<'mcx>>>,
    /// `List *usingClause` — optional using clause for more tables.
    pub usingClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *whereClause` — qualifications.
    pub whereClause: Option<NodePtr<'mcx>>,
    /// `ReturningClause *returningClause`.
    pub returningClause: Option<PgBox<'mcx, ReturningClause<'mcx>>>,
    /// `WithClause *withClause`.
    pub withClause: Option<PgBox<'mcx, WithClause<'mcx>>>,
}

impl DeleteStmt<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `DeleteStmt`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DeleteStmt<'b>> {
        Ok(DeleteStmt {
            relation: match &self.relation {
                Some(r) => Some(mcx::alloc_in(mcx, r.clone_in(mcx)?)?),
                None => None,
            },
            usingClause: copy_node_vec(&self.usingClause, mcx)?,
            whereClause: copy_opt_node(&self.whereClause, mcx)?,
            returningClause: match &self.returningClause {
                Some(r) => Some(mcx::alloc_in(mcx, r.clone_in(mcx)?)?),
                None => None,
            },
            withClause: match &self.withClause {
                Some(w) => Some(mcx::alloc_in(mcx, w.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
}

/// `UpdateStmt` (`nodes/parsenodes.h`) — the raw `UPDATE` statement.
#[derive(Debug)]
pub struct UpdateStmt<'mcx> {
    /// `RangeVar *relation` — relation to update.
    pub relation: Option<PgBox<'mcx, RangeVar<'mcx>>>,
    /// `List *targetList` — the target list (of ResTarget).
    pub targetList: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *whereClause` — qualifications.
    pub whereClause: Option<NodePtr<'mcx>>,
    /// `List *fromClause` — optional from clause for more tables.
    pub fromClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `ReturningClause *returningClause`.
    pub returningClause: Option<PgBox<'mcx, ReturningClause<'mcx>>>,
    /// `WithClause *withClause`.
    pub withClause: Option<PgBox<'mcx, WithClause<'mcx>>>,
}

impl UpdateStmt<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `UpdateStmt`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<UpdateStmt<'b>> {
        Ok(UpdateStmt {
            relation: match &self.relation {
                Some(r) => Some(mcx::alloc_in(mcx, r.clone_in(mcx)?)?),
                None => None,
            },
            targetList: copy_node_vec(&self.targetList, mcx)?,
            whereClause: copy_opt_node(&self.whereClause, mcx)?,
            fromClause: copy_node_vec(&self.fromClause, mcx)?,
            returningClause: match &self.returningClause {
                Some(r) => Some(mcx::alloc_in(mcx, r.clone_in(mcx)?)?),
                None => None,
            },
            withClause: match &self.withClause {
                Some(w) => Some(mcx::alloc_in(mcx, w.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
}

/// `MergeStmt` (`nodes/parsenodes.h`) — the raw `MERGE` statement.
#[derive(Debug)]
pub struct MergeStmt<'mcx> {
    /// `RangeVar *relation` — target relation to merge into.
    pub relation: Option<PgBox<'mcx, RangeVar<'mcx>>>,
    /// `Node *sourceRelation` — source relation.
    pub sourceRelation: Option<NodePtr<'mcx>>,
    /// `Node *joinCondition` — join condition between source and target.
    pub joinCondition: Option<NodePtr<'mcx>>,
    /// `List *mergeWhenClauses` — list of MergeWhenClause(es).
    pub mergeWhenClauses: PgVec<'mcx, NodePtr<'mcx>>,
    /// `ReturningClause *returningClause`.
    pub returningClause: Option<PgBox<'mcx, ReturningClause<'mcx>>>,
    /// `WithClause *withClause`.
    pub withClause: Option<PgBox<'mcx, WithClause<'mcx>>>,
}

impl MergeStmt<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `MergeStmt`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<MergeStmt<'b>> {
        Ok(MergeStmt {
            relation: match &self.relation {
                Some(r) => Some(mcx::alloc_in(mcx, r.clone_in(mcx)?)?),
                None => None,
            },
            sourceRelation: copy_opt_node(&self.sourceRelation, mcx)?,
            joinCondition: copy_opt_node(&self.joinCondition, mcx)?,
            mergeWhenClauses: copy_node_vec(&self.mergeWhenClauses, mcx)?,
            returningClause: match &self.returningClause {
                Some(r) => Some(mcx::alloc_in(mcx, r.clone_in(mcx)?)?),
                None => None,
            },
            withClause: match &self.withClause {
                Some(w) => Some(mcx::alloc_in(mcx, w.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
}

/// `SelectStmt` (`nodes/parsenodes.h`) — the raw `SELECT` (or VALUES, or set-op
/// tree) statement.
#[derive(Debug)]
pub struct SelectStmt<'mcx> {
    // --- "leaf" SelectStmt fields ---
    /// `List *distinctClause` — NULL, DISTINCT ON exprs, or all (DISTINCT).
    pub distinctClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `IntoClause *intoClause` — target for SELECT INTO.
    pub intoClause: Option<NodePtr<'mcx>>,
    /// `List *targetList` — the target list (of ResTarget).
    pub targetList: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *fromClause` — the FROM clause.
    pub fromClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *whereClause` — WHERE qualification.
    pub whereClause: Option<NodePtr<'mcx>>,
    /// `List *groupClause` — GROUP BY clauses.
    pub groupClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `bool groupDistinct` — is this GROUP BY DISTINCT?
    pub groupDistinct: bool,
    /// `Node *havingClause` — HAVING conditional-expression.
    pub havingClause: Option<NodePtr<'mcx>>,
    /// `List *windowClause` — WINDOW window_name AS (...), ...
    pub windowClause: PgVec<'mcx, NodePtr<'mcx>>,
    // --- VALUES list (leaf) ---
    /// `List *valuesLists` — untransformed list of expression lists.
    pub valuesLists: PgVec<'mcx, NodePtr<'mcx>>,
    // --- leaf + upper-level ---
    /// `List *sortClause` — sort clause (a list of SortBy's).
    pub sortClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *limitOffset` — # of result tuples to skip.
    pub limitOffset: Option<NodePtr<'mcx>>,
    /// `Node *limitCount` — # of result tuples to return.
    pub limitCount: Option<NodePtr<'mcx>>,
    /// `LimitOption limitOption` — limit type.
    pub limitOption: LimitOption,
    /// `List *lockingClause` — FOR UPDATE (list of LockingClause's).
    pub lockingClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `WithClause *withClause`.
    pub withClause: Option<PgBox<'mcx, WithClause<'mcx>>>,
    // --- upper-level (set-op tree) ---
    /// `SetOperation op` — type of set op.
    pub op: SetOperation,
    /// `bool all` — ALL specified?
    pub all: bool,
    /// `SelectStmt *larg` — left child.
    pub larg: Option<PgBox<'mcx, SelectStmt<'mcx>>>,
    /// `SelectStmt *rarg` — right child.
    pub rarg: Option<PgBox<'mcx, SelectStmt<'mcx>>>,
}

impl SelectStmt<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `SelectStmt`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<SelectStmt<'b>> {
        Ok(SelectStmt {
            distinctClause: copy_node_vec(&self.distinctClause, mcx)?,
            intoClause: copy_opt_node(&self.intoClause, mcx)?,
            targetList: copy_node_vec(&self.targetList, mcx)?,
            fromClause: copy_node_vec(&self.fromClause, mcx)?,
            whereClause: copy_opt_node(&self.whereClause, mcx)?,
            groupClause: copy_node_vec(&self.groupClause, mcx)?,
            groupDistinct: self.groupDistinct,
            havingClause: copy_opt_node(&self.havingClause, mcx)?,
            windowClause: copy_node_vec(&self.windowClause, mcx)?,
            valuesLists: copy_node_vec(&self.valuesLists, mcx)?,
            sortClause: copy_node_vec(&self.sortClause, mcx)?,
            limitOffset: copy_opt_node(&self.limitOffset, mcx)?,
            limitCount: copy_opt_node(&self.limitCount, mcx)?,
            limitOption: self.limitOption,
            lockingClause: copy_node_vec(&self.lockingClause, mcx)?,
            withClause: match &self.withClause {
                Some(w) => Some(mcx::alloc_in(mcx, w.clone_in(mcx)?)?),
                None => None,
            },
            op: self.op,
            all: self.all,
            larg: match &self.larg {
                Some(l) => Some(mcx::alloc_in(mcx, l.clone_in(mcx)?)?),
                None => None,
            },
            rarg: match &self.rarg {
                Some(r) => Some(mcx::alloc_in(mcx, r.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
}
