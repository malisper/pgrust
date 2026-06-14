//! Utility-statement parse nodes consumed by the command drivers
//! (`nodes/parsenodes.h`), trimmed to the fields the PREPARE / EXECUTE /
//! DEALLOCATE / EXPLAIN drivers read.
//!
//! prepare.c (and its peer command drivers) thread several live parse/plan
//! trees that belong to unported subsystems (the plan cache's
//! `CachedPlanSource`/`CachedPlan`, the parser's parameter-expression `Node`s,
//! the executor's `ParamListInfo`, the portal machinery's `Portal`). Where the
//! driver only passes such a value through to the owning subsystem without
//! dereferencing it, it crosses as an opaque handle newtype here, mirroring
//! the C pointer to the owner-defined struct. These resolve to the real type
//! when their owning unit lands.

use mcx::{Mcx, PgBox};
use types_core::primitive::TimestampTz;
use types_error::PgResult;
use types_opclass::TypeName;

use crate::nodes::Node;

/// Opaque handle to a `CachedPlanSource *` (`utils/plancache.h`). `NULL` is the
/// `None` of the carrier. Owned by the unported plancache unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CachedPlanSourceHandle(pub u64);

impl CachedPlanSourceHandle {
    /// The `NULL` plan source.
    pub const NULL: CachedPlanSourceHandle = CachedPlanSourceHandle(0);
}

/// Opaque handle to a `CachedPlan *` (`utils/plancache.h`). Owned by the
/// unported plancache unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CachedPlanHandle(pub u64);

impl CachedPlanHandle {
    /// The `NULL` cached plan.
    pub const NULL: CachedPlanHandle = CachedPlanHandle(0);
}

/// Opaque handle to a `ParamListInfo` (`nodes/params.h`). `NULL` is the C NULL.
/// Owned by the unported params/executor unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ParamListInfoHandle(pub u64);

impl ParamListInfoHandle {
    /// The `NULL` parameter list.
    pub const NULL: ParamListInfoHandle = ParamListInfoHandle(0);
}

/// Opaque handle to a `Portal` (`utils/portal.h`, name-keyed in portalmem.c).
/// Owned by the unported portalmem unit.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PortalHandle(pub alloc::string::String);

/// Opaque handle to a `MemoryContext` (`utils/mmgr`). The portal's
/// `portalContext` is owned by the unported portalmem unit, so the driver only
/// threads the handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MemoryContextHandle(pub u64);

impl MemoryContextHandle {
    /// The `NULL` memory context.
    pub const NULL: MemoryContextHandle = MemoryContextHandle(0);
}

/// Opaque handle to a `ResourceOwner` (`utils/resowner.h`). Owned by the
/// unported resowner unit; the query-lifecycle model replaces it with owner
/// values, but until that owner lands the driver threads the handle (C NULL ==
/// `NULL`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ResourceOwnerHandle(pub u64);

impl ResourceOwnerHandle {
    /// The `NULL` resource owner.
    pub const NULL: ResourceOwnerHandle = ResourceOwnerHandle(0);
}

/// Opaque handle to a `DestReceiver *` (`tcop/dest.h`). Owned by the caller of
/// the EXECUTE driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DestReceiverHandle(pub u64);

impl DestReceiverHandle {
    /// The `NULL` dest receiver.
    pub const NULL: DestReceiverHandle = DestReceiverHandle(0);
}

/// Opaque handle to a `QueryCompletion *` (`tcop/cmdtag.h`). Owned by the
/// caller of the EXECUTE driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QueryCompletionHandle(pub u64);

impl QueryCompletionHandle {
    /// The `NULL` query completion.
    pub const NULL: QueryCompletionHandle = QueryCompletionHandle(0);
}

/// `CommandTag` (`tcop/cmdtag.h`) — the statement's command-tag enumerator,
/// carried as its integer value (the generated `cmdtaglist.h` order).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CommandTag(pub i32);

/// `ParseState *` (`parser/parse_node.h`), trimmed to what the command drivers
/// read. The full struct has ~36 fields; the PREPARE/EXECUTE/EXPLAIN drivers
/// only read `p_sourcetext` and `p_queryEnv`.
#[derive(Debug)]
pub struct ParseState<'mcx> {
    /// `p_sourcetext` — the source text of the current query (may be `None`).
    pub p_sourcetext: Option<mcx::PgString<'mcx>>,
    /// `p_queryEnv` — the QueryEnvironment, or `None` for the default.
    pub p_queryEnv: Option<PgBox<'mcx, crate::queryenvironment::QueryEnvironment<'mcx>>>,
}

/// `RawStmt` (`nodes/parsenodes.h`) — the wrapper a raw parse tree is placed
/// in before parse analysis, recording the statement's source-text span.
///
/// ```c
/// typedef struct RawStmt {
///     NodeTag type;
///     Node *stmt;        /* raw parse tree */
///     ParseLoc stmt_location;  /* start location, or -1 if unknown */
///     ParseLoc stmt_len;       /* length in bytes; 0 means "rest of string" */
/// } RawStmt;
/// ```
#[derive(Debug)]
pub struct RawStmt<'mcx> {
    /// `Node *stmt` — the contained raw parse tree.
    pub stmt: PgBox<'mcx, Node<'mcx>>,
    /// `ParseLoc stmt_location` — start location, or -1 if unknown.
    pub stmt_location: i32,
    /// `ParseLoc stmt_len` — length in bytes; 0 means "rest of string".
    pub stmt_len: i32,
}

impl RawStmt<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `RawStmt`). The contained raw
    /// parse tree is copied via `Node::clone_in`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RawStmt<'b>> {
        Ok(RawStmt {
            stmt: mcx::alloc_in(mcx, self.stmt.clone_in(mcx)?)?,
            stmt_location: self.stmt_location,
            stmt_len: self.stmt_len,
        })
    }
}

/// `PrepareStmt` (`nodes/parsenodes.h`) — the parsed `PREPARE` statement.
#[derive(Debug)]
pub struct PrepareStmt<'mcx> {
    /// `char *name` — name of plan, arbitrary (`None` / empty = the protocol
    /// unnamed statement, which PREPARE rejects).
    pub name: Option<mcx::PgString<'mcx>>,
    /// `List *argtypes` — type names for parameters. Each is a concrete
    /// `TypeName` (the real fields the grammar's `makeTypeName` produced:
    /// `names`/`typeOid`/`setof`/`pct_type`/`typemod`/`location`); the PREPARE
    /// driver never inspects them, it hands each straight to `typenameTypeId`.
    pub argtypes: mcx::PgVec<'mcx, TypeName>,
    /// `Node *query` — the query itself (as a raw parse tree).
    pub query: Option<PgBox<'mcx, Node<'mcx>>>,
}

impl PrepareStmt<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `PrepareStmt`). `name` is a
    /// `char *`, `argtypes` a `List *` of `TypeName` (lifetime-free, plain
    /// clone), and `query` the raw parse tree (`Node::clone_in`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PrepareStmt<'b>> {
        let mut argtypes = mcx::vec_with_capacity_in(mcx, self.argtypes.len())?;
        for t in self.argtypes.iter() {
            argtypes.push(t.clone());
        }
        Ok(PrepareStmt {
            name: match &self.name {
                Some(s) => Some(s.clone_in(mcx)?),
                None => None,
            },
            argtypes,
            query: match &self.query {
                Some(q) => Some(mcx::alloc_in(mcx, q.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
}

/// `ExecuteStmt` (`nodes/parsenodes.h`) — the parsed `EXECUTE` statement.
#[derive(Debug)]
pub struct ExecuteStmt<'mcx> {
    /// `char *name` — the name of the prepared statement.
    pub name: Option<mcx::PgString<'mcx>>,
    /// `List *params` — values to assign to parameters (raw parser output).
    pub params: mcx::PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
}

impl ExecuteStmt<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `ExecuteStmt`). `name` is a
    /// `char *`; `params` is a `List *` of raw parse trees, each copied via
    /// `Node::clone_in`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ExecuteStmt<'b>> {
        let mut params = mcx::vec_with_capacity_in(mcx, self.params.len())?;
        for p in self.params.iter() {
            params.push(mcx::alloc_in(mcx, p.clone_in(mcx)?)?);
        }
        Ok(ExecuteStmt {
            name: match &self.name {
                Some(s) => Some(s.clone_in(mcx)?),
                None => None,
            },
            params,
        })
    }
}

/// `DeallocateStmt` (`nodes/parsenodes.h`) — the parsed `DEALLOCATE` statement.
#[derive(Debug)]
pub struct DeallocateStmt<'mcx> {
    /// `char *name` — the name of the prepared statement (`None` == DEALLOCATE
    /// ALL).
    pub name: Option<mcx::PgString<'mcx>>,
    /// `bool isall` — true if DEALLOCATE ALL (kept for parity; the driver
    /// branches on `name`).
    pub isall: bool,
}

impl DeallocateStmt<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `DeallocateStmt`). `name` is a
    /// `char *`; `isall` a scalar flag.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DeallocateStmt<'b>> {
        Ok(DeallocateStmt {
            name: match &self.name {
                Some(s) => Some(s.clone_in(mcx)?),
                None => None,
            },
            isall: self.isall,
        })
    }
}

/// `IntoClause` (`nodes/primnodes.h`) — target-relation spec for CREATE TABLE
/// AS / SELECT INTO. The EXECUTE/EXPLAIN drivers thread it through
/// `GetIntoRelEFlags` and read `skipData`; the rest is owned by createas.
#[derive(Debug)]
pub struct IntoClause<'mcx> {
    /// `bool skipData` — true for WITH NO DATA.
    pub skipData: bool,
    /// The remaining IntoClause fields the createas unit owns, threaded as the
    /// opaque parser node payload.
    pub node: PgBox<'mcx, Node<'mcx>>,
}

impl IntoClause<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `IntoClause`). `skipData` is a
    /// scalar; the remaining createas-owned fields cross as the opaque parser
    /// node payload, copied via `Node::clone_in`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<IntoClause<'b>> {
        Ok(IntoClause {
            skipData: self.skipData,
            node: mcx::alloc_in(mcx, self.node.clone_in(mcx)?)?,
        })
    }
}

/// `ExplainState *` (`commands/explain_state.h`), trimmed to the flags the
/// EXPLAIN EXECUTE driver reads. The full struct is owned by the explain unit;
/// the driver only reads `memory` / `buffers` and threads the rest through the
/// explain seams.
#[derive(Debug)]
pub struct ExplainState<'mcx> {
    /// `bool memory` — print planner memory consumption.
    pub memory: bool,
    /// `bool buffers` — print buffer usage.
    pub buffers: bool,
    /// The remaining ExplainState the explain unit owns, threaded as an opaque
    /// handle the explain seams resolve.
    pub node: PgBox<'mcx, Node<'mcx>>,
}

/// A prepared statement's stored data (`commands/prepare.h`
/// `PreparedStatement`). `stmt_name[NAMEDATALEN]` is the per-backend hash key;
/// the rest are a thin veneer over the plancache entry handle.
#[derive(Clone, Debug)]
pub struct PreparedStatement {
    /// `stmt_name[NAMEDATALEN]` — the dynahash key (truncated to
    /// `NAMEDATALEN-1`). Owned by the per-backend hash table, so a plain
    /// `String` (a backend-lifetime global per docs/mctx-design.md decision 5).
    pub stmt_name: alloc::string::String,
    /// `CachedPlanSource *plansource` — the actual cached plan.
    pub plansource: CachedPlanSourceHandle,
    /// `bool from_sql` — prepared via SQL, not the FE/BE protocol?
    pub from_sql: bool,
    /// `TimestampTz prepare_time` — the time when the stmt was prepared.
    pub prepare_time: TimestampTz,
}
