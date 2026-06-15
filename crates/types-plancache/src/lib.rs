//! Signature vocabulary for `backend-utils-cache-plancache`
//! (`utils/cache/plancache.h`, `utils/plancache.c`).
//!
//! plancache keeps its own machinery in-crate (the saved-source list, the
//! cached-expression list, the `CachedPlan` reference counting, the
//! generic/custom plan cost accounting, the custom-vs-generic policy, the
//! result-tupdesc bookkeeping, and the full control flow of every
//! revalidate/check/build/invalidate routine). Only calls into *other*
//! subsystems cross a seam.
//!
//! The querytree (`List *` of `Query`), the planned-statement list (`List *`
//! of `PlannedStmt`), the raw/analyzed parse trees, the search-path matcher
//! (`namespace.c`), the result `TupleDesc` (`tupdesc.c`), and the parameter /
//! resource-owner / query-environment values are all owned by subsystems that
//! are not yet ported. plancache never constructs or owns their internals; it
//! refers to them by opaque identity and reads their fields through owner
//! seam accessors — exactly the boundary `plancache.c` itself draws. Each such
//! identity is a newtype token here (inherited opacity; no bare integer
//! aliases). When an owner lands it replaces the token with its real type and
//! updates the seam signatures.

#![no_std]
#![allow(non_camel_case_types)]

use types_core::primitive::Oid;

/// `PlanCacheMode` — values for the `plan_cache_mode` GUC (`plancache.h`).
pub type PlanCacheMode = i32;
/// `PLAN_CACHE_MODE_AUTO`.
pub const PLAN_CACHE_MODE_AUTO: PlanCacheMode = 0;
/// `PLAN_CACHE_MODE_FORCE_GENERIC_PLAN`.
pub const PLAN_CACHE_MODE_FORCE_GENERIC_PLAN: PlanCacheMode = 1;
/// `PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN`.
pub const PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN: PlanCacheMode = 2;

/// `CACHEDPLANSOURCE_MAGIC` (`plancache.h`).
pub const CACHEDPLANSOURCE_MAGIC: i32 = 195_726_186;
/// `CACHEDPLAN_MAGIC` (`plancache.h`).
pub const CACHEDPLAN_MAGIC: i32 = 953_717_834;
/// `CACHEDEXPR_MAGIC` (`plancache.h`).
pub const CACHEDEXPR_MAGIC: i32 = 838_275_847;

/// `CURSOR_OPT_CUSTOM_PLAN` (`parsenodes.h`).
pub const CURSOR_OPT_CUSTOM_PLAN: i32 = 0x0400;
/// `CURSOR_OPT_GENERIC_PLAN` (`parsenodes.h`).
pub const CURSOR_OPT_GENERIC_PLAN: i32 = 0x0200;

/// `RTE_RELATION` (`parsenodes.h`).
pub const RTE_RELATION: i32 = 0;
/// `RTE_SUBQUERY` (`parsenodes.h`).
pub const RTE_SUBQUERY: i32 = 1;

/// `CMD_UTILITY` (`nodes.h`), the only `CmdType` plancache compares against.
pub const CMD_UTILITY: i32 = 6;

/// `FirstNormalTransactionId` (`transam.h`).
pub const FIRST_NORMAL_TRANSACTION_ID: u32 = 3;

/// `CommandTag` (`tcop/cmdtag.h`) — a C `int`-sized enum that plancache only
/// stores by value (it never inspects the tag). The full tag table is owned by
/// `tcop/cmdtag.c`; here it is the opaque scalar identity carried through.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct CommandTag(pub i32);

macro_rules! opaque_handle {
    ($(#[$m:meta])* $name:ident, $null_name:ident) => {
        $(#[$m])*
        #[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
        pub struct $name(pub u64);

        impl $name {
            /// The NULL / NIL value (`0`).
            pub const $null_name: $name = $name(0);
            /// Whether this is NULL / NIL.
            #[inline]
            pub fn is_null(self) -> bool {
                self.0 == 0
            }
        }
    };
}

opaque_handle!(
    /// Implementation-owned querytree `List *` (`List *query_list`, the
    /// rewritten `Query` nodes), or the transient list a planner pass returns.
    /// `0` models NIL. Owned by the parser/rewrite subsystem.
    QueryListHandle, NIL
);

impl QueryListHandle {
    /// Whether this is NIL (alias of [`is_null`](Self::is_null)).
    #[inline]
    pub fn is_nil(self) -> bool {
        self.0 == 0
    }
}

opaque_handle!(
    /// One querytree element (`Query *`). `0` is NULL. Owned by parser/rewrite.
    QueryHandle, NULL
);

opaque_handle!(
    /// The cached plan's planned-statement list (`List *stmt_list`, the
    /// `PlannedStmt` nodes from `pg_plan_queries`). `0` is NIL. Owned by the
    /// planner; plancache reads its elements through the planner seam.
    PlannedStmtListHandle, NIL
);

opaque_handle!(
    /// One `PlannedStmt *` element of a [`PlannedStmtListHandle`]. `0` is NULL.
    PlannedStmtHandle, NULL
);

opaque_handle!(
    /// Analyzed query (`Query *analyzed_parse_tree`). `0` is NULL. Owned by
    /// parser/analyze.
    AnalyzedQueryHandle, NULL
);

opaque_handle!(
    /// Raw parse tree (`RawStmt *raw_parse_tree`). `0` is NULL. Owned by the
    /// raw parser; plancache copies it via the node-copy seam.
    RawStmtHandle, NULL
);

opaque_handle!(
    /// An expression node (`Node *expr` of a `CachedExpression`). `0` is NULL.
    ExprHandle, NULL
);

opaque_handle!(
    /// Target `List *` (`targetList`/`returningList`). `0` is NIL.
    TargetListHandle, NIL
);

opaque_handle!(
    /// Utility statement node (`Node *utilityStmt`). `0` is NULL.
    UtilityStmtHandle, NULL
);

opaque_handle!(
    /// Implementation-owned `SearchPathMatcher *` (`namespace.c`). `0` is NULL.
    SearchPathMatcherHandle, NULL
);

opaque_handle!(
    /// Implementation-owned `TupleDesc` (`tupdesc.c`). `0` models a NULL
    /// descriptor (statement returns no tuples).
    TupleDescHandle, NULL
);

opaque_handle!(
    /// `ResourceOwner` (`resowner.c`). `0` is NULL.
    ResourceOwnerHandle, NULL
);

opaque_handle!(
    /// `ParamListInfo` boundParams. `0` is NULL.
    ParamListInfoHandle, NULL
);

opaque_handle!(
    /// `QueryEnvironment *`. `0` is NULL.
    QueryEnvHandle, NULL
);

opaque_handle!(
    /// `MemoryContext` identity (`memutils.c`). `0` is NULL. The plancache
    /// memory-context manipulations (create / switch / set-parent / delete /
    /// identifier) are owned by the mctx-remainder subsystem; plancache
    /// threads this token through the mcxt seam.
    CtxId, NULL
);

impl CtxId {
    /// Whether a context handle is present.
    #[inline]
    pub fn is_some(self) -> bool {
        self.0 != 0
    }
}

opaque_handle!(
    /// A parser-setup hook + its `void *arg`, threaded opaquely from
    /// `CompleteCachedPlan` callers to the analyze seam. `0` means "no hook".
    ParserSetupHandle, NONE
);

impl ParserSetupHandle {
    /// Whether a hook is present.
    #[inline]
    pub fn is_some(self) -> bool {
        self.0 != 0
    }
}

opaque_handle!(
    /// A post-rewrite hook + its `void *arg`. `0` means "no hook".
    PostRewriteHandle, NONE
);

impl PostRewriteHandle {
    /// Whether a hook is present.
    #[inline]
    pub fn is_some(self) -> bool {
        self.0 != 0
    }
}

/// One `RangeTblEntry`'s fields that plancache reads
/// (`AcquireExecutorLocks`/`ScanQueryForLocks`/
/// `CachedPlanAllowsSimpleValidityCheck`), bundled into a single seam crossing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RteFields {
    /// `rte->rtekind` (`RTE_RELATION`/`RTE_SUBQUERY`/...).
    pub rtekind: i32,
    /// `rte->relid`.
    pub relid: Oid,
    /// `rte->rellockmode`.
    pub rellockmode: i32,
    /// `rte->subquery` (`Query *`), NULL if none.
    pub subquery: QueryHandle,
}

/// The `(cacheId, hashValue)` pair of a `PlanInvalItem` (`plannodes.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InvalItemKey {
    /// `PlanInvalItem.cacheId` — a syscache ID.
    pub cache_id: i32,
    /// `PlanInvalItem.hashValue`.
    pub hash_value: u32,
}

/// `RelcacheCallbackFunction` (`inval.h`) — `void (*)(Datum arg, Oid relid)`;
/// the `Datum arg` plancache registers is always 0, so it is dropped.
pub type RelcacheCallbackFn = fn(relid: Oid);
/// `SyscacheCallbackFunction` (`inval.h`) — `void (*)(Datum, int, uint32)`;
/// the `Datum arg` is always 0, so it is dropped.
pub type SyscacheCallbackFn = fn(cacheid: i32, hashvalue: u32);

/// The syscaches `InitPlanCache` registers callbacks for (`utils/syscache.h`).
/// The integer `SysCacheIdentifier` values are owned by the syscache
/// subsystem, so they are resolved through a seam rather than hard-coded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SysCacheId {
    /// `PROCOID`.
    ProcOid,
    /// `TYPEOID`.
    TypeOid,
    /// `NAMESPACEOID`.
    NamespaceOid,
    /// `OPEROID`.
    OperOid,
    /// `AMOPOPID`.
    AmOpOpId,
    /// `FOREIGNSERVEROID`.
    ForeignServerOid,
    /// `FOREIGNDATAWRAPPEROID`.
    ForeignDataWrapperOid,
}

/// `PortalStrategy` (`portal.h`) — only the variants
/// `PlanCacheComputeResultDesc` distinguishes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PortalStrategy {
    /// `PORTAL_ONE_SELECT`.
    OneSelect,
    /// `PORTAL_ONE_RETURNING`.
    OneReturning,
    /// `PORTAL_ONE_MOD_WITH`.
    OneModWith,
    /// `PORTAL_UTIL_SELECT`.
    UtilSelect,
    /// `PORTAL_MULTI_QUERY`.
    MultiQuery,
}
