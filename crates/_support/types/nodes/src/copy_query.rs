//! The parse/plan/execute vocabulary the COPY-(query)-TO driver consumes
//! (`nodes/parsenodes.h`, `nodes/plannodes.h`, `executor/execdesc.h`), trimmed
//! to the fields copyto.c reads. The parser/planner/executor units that own
//! these types are unported; copyto reaches their functions through seams and
//! reads only these fields off the returned values.

use mcx::{Mcx, PgBox, PgString, PgVec};
use ::types_core::primitive::Oid;
use ::types_error::PgResult;

use crate::nodelimit::LimitOption;
use crate::nodes::{CmdType, NodePtr};
use crate::parsenodes::{RTEPermissionInfo, RangeTblEntry};
use crate::primnodes::{Expr, TargetEntry};
use crate::rawnodes::{FromExpr, OnConflictExpr};

/// `CURSOR_OPT_PARALLEL_OK` (`nodes/parsenodes.h`) — parallel mode OK.
pub const CURSOR_OPT_PARALLEL_OK: i32 = 0x0800;

/// `ParseState` (`parser/parse_node.h`). Unified (K1 phase 4) onto the single
/// canonical full struct in [`crate::parsestmt`]; the COPY drivers read only
/// `p_sourcetext` (the original query string passed to analysis and planning),
/// which is now an `Option<PgString>` (the C field is a possibly-NULL
/// `const char *`). Re-exported for type identity — no behavior change.
pub use crate::parsestmt::ParseState;

/// `QuerySource` (`nodes/parsenodes.h`) — where a rewritten query came from.
/// Values are PostgreSQL 18.3's enumeration order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum QuerySource {
    /// `QSRC_ORIGINAL` — original parsetree (explicit query).
    QSRC_ORIGINAL = 0,
    /// `QSRC_PARSER` — added by parse analysis (now unused).
    QSRC_PARSER = 1,
    /// `QSRC_INSTEAD_RULE` — added by unconditional INSTEAD rule.
    QSRC_INSTEAD_RULE = 2,
    /// `QSRC_QUAL_INSTEAD_RULE` — added by conditional INSTEAD rule.
    QSRC_QUAL_INSTEAD_RULE = 3,
    /// `QSRC_NON_INSTEAD_RULE` — added by non-INSTEAD rule.
    QSRC_NON_INSTEAD_RULE = 4,
}

/// `Query` (`nodes/parsenodes.h`), trimmed to the fields the COPY-(query)-TO
/// validation reads after rewrite.
///
/// Canonical (K1 phase 2) `<'mcx>` trimmed view of `Query`: it carries the
/// post-rewrite fields the analyze/rewrite consumers read (`commandType`,
/// `querySource`, `utilityStmt` tag, returning-list presence). The
/// [`crate::portalcmds::Query`] token is a *distinct* model — a non-`'mcx`
/// `Rc<RefCell<…>>` pass-through whose only inspected field is `commandType`,
/// threaded by-value through the portal's jumble/rewrite/plan seams. The two
/// cannot share one definition: this one is arena-lifetimed and field-bearing,
/// the portalcmds one is a refcounted owned token with a different field set
/// and by-value (non-`'mcx`) consumers (`postgres-seams`, `queryjumble-seams`,
/// `rewritehandler-seams`). Re-exporting either into the other's module would
/// change those signatures, so the portalcmds token stays distinct and
/// documented as such. (Both remain trimmed views of the same C `Query`; the
/// full node model is a later K1 keystone.)
#[derive(Debug)]
pub struct Query<'mcx> {
    /// `CmdType commandType`.
    pub commandType: CmdType,
    /// `QuerySource querySource`.
    pub querySource: QuerySource,
    /// `int64 queryId` — query identifier (set by plugins).
    pub queryId: i64,
    /// `bool canSetTag` — do I set the command result tag?
    pub canSetTag: bool,
    /// `Node *utilityStmt` — non-null if `commandType == CMD_UTILITY`. Read the
    /// node tag via [`crate::nodes::Node::tag`] (e.g. the SELECT-INTO
    /// `CreateTableAsStmt` check copyto performs).
    pub utilityStmt: Option<NodePtr<'mcx>>,
    /// `int resultRelation` — rtable index of target rel for
    /// INSERT/UPDATE/DELETE/MERGE; 0 for SELECT.
    pub resultRelation: i32,
    /// `bool hasAggs` — has aggregates in tlist or havingQual.
    pub hasAggs: bool,
    /// `bool hasWindowFuncs` — has window functions in tlist.
    pub hasWindowFuncs: bool,
    /// `bool hasTargetSRFs` — has set-returning functions in tlist.
    pub hasTargetSRFs: bool,
    /// `bool hasSubLinks` — has subquery `SubLink`.
    pub hasSubLinks: bool,
    /// `bool hasDistinctOn` — `distinctClause` is from DISTINCT ON.
    pub hasDistinctOn: bool,
    /// `bool hasRecursive` — WITH RECURSIVE was specified.
    pub hasRecursive: bool,
    /// `bool hasModifyingCTE` — has INSERT/UPDATE/DELETE/MERGE in WITH.
    pub hasModifyingCTE: bool,
    /// `bool hasForUpdate` — FOR [KEY] UPDATE/SHARE was specified.
    pub hasForUpdate: bool,
    /// `bool hasRowSecurity` — rewriter has applied some RLS policy.
    pub hasRowSecurity: bool,
    /// `bool hasGroupRTE` — parser has added an `RTE_GROUP` RTE.
    pub hasGroupRTE: bool,
    /// `bool isReturn` — is a RETURN statement.
    pub isReturn: bool,
    /// `List *cteList` — WITH list (of `CommonTableExpr`'s).
    pub cteList: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *rtable` — list of range table entries.
    pub rtable: PgVec<'mcx, RangeTblEntry<'mcx>>,
    /// `List *rteperminfos` — list of `RTEPermissionInfo` nodes.
    pub rteperminfos: PgVec<'mcx, RTEPermissionInfo<'mcx>>,
    /// `FromExpr *jointree` — table join tree (FROM and WHERE clauses).
    pub jointree: Option<PgBox<'mcx, FromExpr<'mcx>>>,
    /// `List *mergeActionList` — list of `MergeAction`s for MERGE (only).
    pub mergeActionList: PgVec<'mcx, NodePtr<'mcx>>,
    /// `int mergeTargetRelation` — rtable index of MERGE source target rel.
    pub mergeTargetRelation: i32,
    /// `Node *mergeJoinCondition` — join condition source/target for MERGE.
    /// Although C types it `Node *`, it only ever holds an expression, so this
    /// is the concretely-typed `Option<PgBox<Expr>>` view (matching `targetList`
    /// and the jointree, which are already concretely typed).
    pub mergeJoinCondition: Option<PgBox<'mcx, Expr<'mcx>>>,
    /// `List *targetList` — target list (of `TargetEntry`).
    pub targetList: PgVec<'mcx, TargetEntry<'mcx>>,
    /// `OverridingKind override` — OVERRIDING clause.
    pub r#override: crate::modifytable::OverridingKind,
    /// `OnConflictExpr *onConflict` — ON CONFLICT DO [NOTHING | UPDATE].
    pub onConflict: Option<PgBox<'mcx, OnConflictExpr<'mcx>>>,
    /// `char *returningOldAlias` — alias name for OLD in RETURNING.
    pub returningOldAlias: Option<PgString<'mcx>>,
    /// `char *returningNewAlias` — alias name for NEW in RETURNING.
    pub returningNewAlias: Option<PgString<'mcx>>,
    /// `List *returningList` — return-values list (of `TargetEntry`).
    pub returningList: PgVec<'mcx, TargetEntry<'mcx>>,
    /// `bool has_returning_list` — convenience flag mirroring `returningList !=
    /// NIL`, kept for the COPY-(query)-TO validation that reads it directly.
    pub has_returning_list: bool,
    /// `List *groupClause` — a list of `SortGroupClause`'s.
    pub groupClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `bool groupDistinct` — is the group by clause distinct?
    pub groupDistinct: bool,
    /// `List *groupingSets` — a list of `GroupingSet`'s if present.
    pub groupingSets: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *havingQual` — qualifications applied to groups. C types it
    /// `Node *`, but it only ever holds an expression; concretely typed here.
    pub havingQual: Option<PgBox<'mcx, Expr<'mcx>>>,
    /// `List *windowClause` — a list of `WindowClause`'s.
    pub windowClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *distinctClause` — a list of `SortGroupClause`'s.
    pub distinctClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `List *sortClause` — a list of `SortGroupClause`'s.
    pub sortClause: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *limitOffset` — # of result tuples to skip (int8 expr). C types it
    /// `Node *`, but it only ever holds an expression; concretely typed here.
    pub limitOffset: Option<PgBox<'mcx, Expr<'mcx>>>,
    /// `Node *limitCount` — # of result tuples to return (int8 expr). C types it
    /// `Node *`, but it only ever holds an expression; concretely typed here.
    pub limitCount: Option<PgBox<'mcx, Expr<'mcx>>>,
    /// `LimitOption limitOption` — limit type.
    pub limitOption: LimitOption,
    /// `List *rowMarks` — a list of `RowMarkClause`'s.
    pub rowMarks: PgVec<'mcx, NodePtr<'mcx>>,
    /// `Node *setOperations` — set-operation tree if this is the top level of a
    /// UNION/INTERSECT/EXCEPT query.
    pub setOperations: Option<NodePtr<'mcx>>,
    /// `List *constraintDeps` — `pg_constraint` OIDs the query depends on.
    pub constraintDeps: PgVec<'mcx, Oid>,
    /// `List *withCheckOptions` — a list of `WithCheckOption`'s.
    pub withCheckOptions: PgVec<'mcx, NodePtr<'mcx>>,
    /// `ParseLoc stmt_location` — start location, or -1 if unknown.
    pub stmt_location: i32,
    /// `ParseLoc stmt_len` — length in bytes; 0 means "rest of string".
    pub stmt_len: i32,
    /// Ties the `Query` to the context it (and its node tree) lives in; the
    /// rewrite output is allocated there.
    pub _marker: core::marker::PhantomData<&'mcx ()>,
}

impl<'mcx> Query<'mcx> {
    /// `makeNode(Query)` — a zero-initialized `Query` (C `palloc0`). All scalar
    /// fields take their `0`/`false`/enum-zero default; all `List *` start `NIL`
    /// (empty `PgVec`); all `Node *`/`PgBox` start `NULL` (`None`). The
    /// statement-location fields start at `0` (the C struct image), as in
    /// `makeNode`; `transformTopLevelStmt` overwrites them from the `RawStmt`.
    pub fn new(mcx: Mcx<'mcx>) -> Query<'mcx> {
        Query {
            commandType: CmdType::CMD_UNKNOWN,
            querySource: QuerySource::QSRC_ORIGINAL,
            queryId: 0,
            canSetTag: false,
            utilityStmt: None,
            resultRelation: 0,
            hasAggs: false,
            hasWindowFuncs: false,
            hasTargetSRFs: false,
            hasSubLinks: false,
            hasDistinctOn: false,
            hasRecursive: false,
            hasModifyingCTE: false,
            hasForUpdate: false,
            hasRowSecurity: false,
            hasGroupRTE: false,
            isReturn: false,
            cteList: PgVec::new_in(mcx),
            rtable: PgVec::new_in(mcx),
            rteperminfos: PgVec::new_in(mcx),
            jointree: None,
            mergeActionList: PgVec::new_in(mcx),
            mergeTargetRelation: 0,
            mergeJoinCondition: None,
            targetList: PgVec::new_in(mcx),
            r#override: crate::modifytable::OverridingKind::OVERRIDING_NOT_SET,
            onConflict: None,
            returningOldAlias: None,
            returningNewAlias: None,
            returningList: PgVec::new_in(mcx),
            has_returning_list: false,
            groupClause: PgVec::new_in(mcx),
            groupDistinct: false,
            groupingSets: PgVec::new_in(mcx),
            havingQual: None,
            windowClause: PgVec::new_in(mcx),
            distinctClause: PgVec::new_in(mcx),
            sortClause: PgVec::new_in(mcx),
            limitOffset: None,
            limitCount: None,
            limitOption: LimitOption::LIMIT_OPTION_COUNT,
            rowMarks: PgVec::new_in(mcx),
            setOperations: None,
            constraintDeps: PgVec::new_in(mcx),
            withCheckOptions: PgVec::new_in(mcx),
            stmt_location: 0,
            stmt_len: 0,
            _marker: core::marker::PhantomData,
        }
    }

    /// Deep copy into `mcx` (C: `copyObject` over `Query`). Every `Node`/`List`
    /// subtree is re-homed onto the target context; fallible since copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Query<'b>> {
        Ok(Query {
            commandType: self.commandType,
            querySource: self.querySource,
            queryId: self.queryId,
            canSetTag: self.canSetTag,
            utilityStmt: copy_opt_node(&self.utilityStmt, mcx)?,
            resultRelation: self.resultRelation,
            hasAggs: self.hasAggs,
            hasWindowFuncs: self.hasWindowFuncs,
            hasTargetSRFs: self.hasTargetSRFs,
            hasSubLinks: self.hasSubLinks,
            hasDistinctOn: self.hasDistinctOn,
            hasRecursive: self.hasRecursive,
            hasModifyingCTE: self.hasModifyingCTE,
            hasForUpdate: self.hasForUpdate,
            hasRowSecurity: self.hasRowSecurity,
            hasGroupRTE: self.hasGroupRTE,
            isReturn: self.isReturn,
            cteList: copy_node_vec(&self.cteList, mcx)?,
            rtable: {
                let mut out = ::mcx::vec_with_capacity_in(mcx, self.rtable.len())?;
                for r in self.rtable.iter() {
                    out.push(r.clone_in(mcx)?);
                }
                out
            },
            rteperminfos: {
                let mut out = ::mcx::vec_with_capacity_in(mcx, self.rteperminfos.len())?;
                for r in self.rteperminfos.iter() {
                    out.push(r.clone_in(mcx)?);
                }
                out
            },
            jointree: match &self.jointree {
                Some(j) => Some(::mcx::alloc_in(mcx, j.clone_in(mcx)?)?),
                None => None,
            },
            mergeActionList: copy_node_vec(&self.mergeActionList, mcx)?,
            mergeTargetRelation: self.mergeTargetRelation,
            mergeJoinCondition: copy_opt_expr(&self.mergeJoinCondition, mcx)?,
            targetList: copy_te_vec(&self.targetList, mcx)?,
            r#override: self.r#override,
            onConflict: match &self.onConflict {
                Some(o) => Some(::mcx::alloc_in(mcx, o.clone_in(mcx)?)?),
                None => None,
            },
            returningOldAlias: match &self.returningOldAlias {
                Some(s) => Some(s.clone_in(mcx)?),
                None => None,
            },
            returningNewAlias: match &self.returningNewAlias {
                Some(s) => Some(s.clone_in(mcx)?),
                None => None,
            },
            returningList: copy_te_vec(&self.returningList, mcx)?,
            has_returning_list: self.has_returning_list,
            groupClause: copy_node_vec(&self.groupClause, mcx)?,
            groupDistinct: self.groupDistinct,
            groupingSets: copy_node_vec(&self.groupingSets, mcx)?,
            havingQual: copy_opt_expr(&self.havingQual, mcx)?,
            windowClause: copy_node_vec(&self.windowClause, mcx)?,
            distinctClause: copy_node_vec(&self.distinctClause, mcx)?,
            sortClause: copy_node_vec(&self.sortClause, mcx)?,
            limitOffset: copy_opt_expr(&self.limitOffset, mcx)?,
            limitCount: copy_opt_expr(&self.limitCount, mcx)?,
            limitOption: self.limitOption,
            rowMarks: copy_node_vec(&self.rowMarks, mcx)?,
            setOperations: copy_opt_node(&self.setOperations, mcx)?,
            constraintDeps: {
                let mut out = ::mcx::vec_with_capacity_in(mcx, self.constraintDeps.len())?;
                for x in self.constraintDeps.iter() {
                    out.push(*x);
                }
                out
            },
            withCheckOptions: copy_node_vec(&self.withCheckOptions, mcx)?,
            stmt_location: self.stmt_location,
            stmt_len: self.stmt_len,
            _marker: core::marker::PhantomData,
        })
    }
}

/// Deep-copy an `Option<NodePtr>` (`Node *` field) into `mcx`.
fn copy_opt_node<'b>(
    n: &Option<NodePtr<'_>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<NodePtr<'b>>> {
    match n {
        Some(n) => Ok(Some(::mcx::alloc_in(mcx, n.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// Deep-copy an `Option<PgBox<Expr>>` (an expression-only `Node *` field) into
/// `mcx`.
fn copy_opt_expr<'b>(
    e: &Option<PgBox<'_, Expr<'_>>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<PgBox<'b, Expr<'b>>>> {
    match e {
        Some(e) => Ok(Some(::mcx::alloc_in(mcx, e.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// Deep-copy a `PgVec<NodePtr>` (`List *` of nodes) into `mcx`.
fn copy_node_vec<'b>(
    v: &PgVec<'_, NodePtr<'_>>,
    mcx: Mcx<'b>,
) -> PgResult<PgVec<'b, NodePtr<'b>>> {
    let mut out = ::mcx::vec_with_capacity_in(mcx, v.len())?;
    for n in v.iter() {
        out.push(::mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
    }
    Ok(out)
}

/// Deep-copy a `PgVec<TargetEntry>` into `mcx`.
fn copy_te_vec<'b>(
    v: &PgVec<'_, TargetEntry<'_>>,
    mcx: Mcx<'b>,
) -> PgResult<PgVec<'b, TargetEntry<'b>>> {
    let mut out = ::mcx::vec_with_capacity_in(mcx, v.len())?;
    for te in v.iter() {
        out.push(te.clone_in(mcx)?);
    }
    Ok(out)
}

/// `RawStmt` (`nodes/parsenodes.h`) — the raw parse tree handed to analysis.
/// Opaque to copyto, which only passes it to the analyze-and-rewrite seam.
///
/// Canonicalized (K1 phase 2): the COPY-(query)-TO driver's view of `RawStmt`
/// was a trimmed duplicate (a single opaque `stmt` node). The canonical,
/// C-faithful `RawStmt<'mcx>` lives in [`crate::parsestmt`] (real
/// `stmt: PgBox<Node>` plus `stmt_location`/`stmt_len`). It subsumes this view
/// — copyto only threads the value through to the analyze-and-rewrite seam and
/// never inspects any field — so this path re-exports the canonical type for
/// pure type identity (no behavior change).
pub use crate::parsestmt::RawStmt;

// NOTE: the trimmed `QueryDesc { tupDesc, exec_token }` view that copyto used to
// thread (an opaque executor handle + the result tupdesc) has been RETIRED. The
// QueryDesc de-handle (F1b) re-points both copyto and the portal onto the single
// canonical owned [`crate::querydesc::QueryDesc`] (lifetime-free; its `work`
// bundle owns the `EState`/plan-state tree and the result tupdesc is read via
// `QueryDesc::with_result_tupdesc`). No `exec_token` handle survives.

/// `T_CreateTableAsStmt` (`nodes/nodetags.h`) — value verified against
/// PostgreSQL 18.3's generated enumeration order. Used by the SELECT-INTO check.
pub const T_CreateTableAsStmt: crate::nodes::NodeTag = crate::nodes::NodeTag(242);
