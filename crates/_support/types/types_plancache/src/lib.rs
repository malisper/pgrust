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
//! plancache de-handled its querytree / plan / parse-tree storage in #159
//! STEP C: it now owns `Query` / `PlannedStmt` / `RawStmt` / `Expr` values,
//! cloning them via `clone_in` into private `MemoryContext`s and crossing
//! value seams. The querytree-list / plan-list / analyzed-query / expr /
//! search-path-matcher / result-tupdesc / query-environment / param-list /
//! parser-setup identity tokens that used to live here were retired with that
//! switch (and their orphaned producer pc-seams deleted). What remains is
//! value vocabulary — magic numbers, GUC-mode / cursor-option constants, the
//! inval-item key, the syscache id set, and the inval callback aliases — plus
//! the two raw/utility-statement tokens (`RawStmtHandle` / `UtilityStmtHandle`)
//! that `types-foreigncmds` and `backend-tcop-postgres-seams` still name.

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
/// `tcop/cmdtag.c`; canonically defined in `types_core` and carried through by
/// value here.
pub use types_core::cmdtag::CommandTag;

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

// NOTE (#159 STEP C plancache de-handle prune): the querytree/plan/parse-tree
// identity tokens that used to live here — QueryListHandle, QueryHandle,
// PlannedStmtListHandle, PlannedStmtHandle, AnalyzedQueryHandle, ExprHandle,
// TargetListHandle, SearchPathMatcherHandle, TupleDescHandle, QueryEnvHandle,
// CtxId, ParserSetupHandle, PostRewriteHandle, ParamListInfoHandle — were
// retired once plancache de-handled onto owned node values (it clones
// Query/PlannedStmt/RawStmt/Expr via `clone_in` into private MemoryContexts and
// crosses value seams instead). The orphaned producer pc-seams were deleted
// with them. RawStmtHandle and UtilityStmtHandle stay below because
// types-foreigncmds / backend-tcop-postgres-seams still name them.

opaque_handle!(
    /// Raw parse tree (`RawStmt *raw_parse_tree`). `0` is NULL. Owned by the
    /// raw parser; plancache copies it via the node-copy seam.
    RawStmtHandle, NULL
);

opaque_handle!(
    /// Utility statement node (`Node *utilityStmt`). `0` is NULL.
    UtilityStmtHandle, NULL
);

/// `ResourceOwner` (`resowner.c`) — the one canonical
/// [`types_resowner::ResourceOwner`] handle, re-exported so plancache keeps
/// naming it `ResourceOwnerHandle`.
pub type ResourceOwnerHandle = types_resowner::ResourceOwner;

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
