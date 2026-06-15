//! ABI vocabulary for the remaining `utils/cache` modules.
//!
//! These `#[repr(C)]` structs / enums / constants cross the boundary between
//! the remaining rewritten `backend-utils-cache-*` crates and the rest of the
//! backend.  They mirror the C definitions in
//!   * `src/include/utils/plancache.h`        (CachedPlanSource/CachedPlan/CachedExpression)
//!   * `src/backend/utils/cache/relmapper.c`  (RelMapping/RelMapFile, the shared map)
//!   * `src/include/utils/relmapper.h`         (xl_relmap_update)
//!   * `src/include/tsearch/ts_cache.h`        (TS*CacheEntry, ListDictionary)
//!   * `src/include/utils/evtcache.h`          (EventTriggerEvent, EventTriggerCacheItem)
//!   * `src/include/utils/lsyscache.h`         (OpIndexInterpretation, IOFuncSelector, AttStatsSlot)
//!
//! Layout-critical fields keep their exact C order/width; catalog-side
//! sub-objects this workspace has not yet modeled are held as pointer-width
//! opaque handles (`*mut c_void`), which is ABI-identical to the C pointers
//! they stand in for.  These types are surfaced through a dedicated module
//! (not the `cache::*` glob) to avoid the ambiguous-glob name-collision trap.

use core::ffi::{c_char, c_void};

use crate::access::CompareType;
use crate::bitmapset::Bitmapset;
use crate::guc::dlist_node;
use crate::heaptuple::TupleDesc;
use crate::list::List;
use crate::memory::MemoryContext;
use crate::{Datum, FmgrInfo, Oid, RelFileNumber, TransactionId};

/* ===========================================================================
 * Shared tiny aliases the cache layer needs.
 * ======================================================================== */

/// `CommandTag` — `src/include/tcop/cmdtag.h` declares this as an `enum`
/// (C `int`-sized); plancache stores it by value.  Modeled as the underlying
/// integer to preserve the ABI without dragging in the full enum here.
pub type CommandTag = core::ffi::c_int;

/// `ResourceOwner` — opaque handle (`src/include/utils/resowner.h`); the cache
/// layer only ever passes it through to the resowner subsystem.
pub type ResourceOwner = *mut c_void;

/// `QueryEnvironment *` — opaque handle (`src/include/utils/queryenvironment.h`).
pub type QueryEnvironmentPtr = *mut c_void;

/// `SearchPathMatcher *` — opaque handle (`src/include/catalog/namespace.h`).
pub type SearchPathMatcherPtr = *mut c_void;

/// `RawStmt *` — opaque handle (`src/include/nodes/parsenodes.h`).
pub type RawStmtPtr = *mut c_void;

// `QueryPtr` (`Query *`, opaque) already lives in `pathnodes` and is reachable
// from the crate root; we reuse it rather than re-defining (ambiguous-glob trap).
pub use crate::pathnodes::QueryPtr;

/// `Node *` — opaque handle (`src/include/nodes/nodes.h`); used here for the
/// planned-expression pointer in `CachedExpression`.
pub type NodePtr = *mut c_void;

/// `ParserSetupHook` (params.h) re-exported here so the plancache crate can
/// name it at the crate root alongside the rest of its ABI vocabulary.
pub use crate::params::ParserSetupHook;

/// `PostRewriteHook` — `void (*)(List *querytree_list, void *arg)` (plancache.h).
pub type PostRewriteHook =
    Option<unsafe extern "C" fn(querytree_list: *mut List, arg: *mut c_void)>;

/* ===========================================================================
 * plancache.h
 * ======================================================================== */

/// `CACHEDPLANSOURCE_MAGIC`.
pub const CACHEDPLANSOURCE_MAGIC: i32 = 195_726_186;
/// `CACHEDPLAN_MAGIC`.
pub const CACHEDPLAN_MAGIC: i32 = 953_717_834;
/// `CACHEDEXPR_MAGIC`.
pub const CACHEDEXPR_MAGIC: i32 = 838_275_847;

/// `PlanCacheMode` — values for the `plan_cache_mode` GUC.
pub type PlanCacheMode = core::ffi::c_uint;
/// `PLAN_CACHE_MODE_AUTO`.
pub const PLAN_CACHE_MODE_AUTO: PlanCacheMode = 0;
/// `PLAN_CACHE_MODE_FORCE_GENERIC_PLAN`.
pub const PLAN_CACHE_MODE_FORCE_GENERIC_PLAN: PlanCacheMode = 1;
/// `PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN`.
pub const PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN: PlanCacheMode = 2;

/// `CachedPlanSource` (plancache.h) — a cached, possibly re-planned, query.
#[repr(C)]
pub struct CachedPlanSource {
    /// should equal `CACHEDPLANSOURCE_MAGIC`.
    pub magic: i32,
    /// output of `raw_parser()`, or NULL (`RawStmt *`).
    pub raw_parse_tree: RawStmtPtr,
    /// analyzed parse tree, or NULL (`Query *`).
    pub analyzed_parse_tree: QueryPtr,
    /// source text of query.
    pub query_string: *const c_char,
    /// command tag for query.
    pub commandTag: CommandTag,
    /// array of parameter type OIDs, or NULL.
    pub param_types: *mut Oid,
    /// length of `param_types` array.
    pub num_params: i32,
    /// alternative parameter spec method.
    pub parserSetup: ParserSetupHook,
    /// arg for `parserSetup`.
    pub parserSetupArg: *mut c_void,
    /// see `SetPostRewriteHook`.
    pub postRewrite: PostRewriteHook,
    /// arg for `postRewrite`.
    pub postRewriteArg: *mut c_void,
    /// cursor options used for planning.
    pub cursor_options: i32,
    /// disallow change in result tupdesc?
    pub fixed_result: bool,
    /// result type; NULL = doesn't return tuples.
    pub resultDesc: TupleDesc,
    /// memory context holding all above.
    pub context: MemoryContext,
    /// list of `Query` nodes, or NIL if not valid.
    pub query_list: *mut List,
    /// OIDs of relations the queries depend on.
    pub relationOids: *mut List,
    /// other dependencies, as `PlanInvalItems`.
    pub invalItems: *mut List,
    /// `search_path` used for parsing and planning.
    pub search_path: SearchPathMatcherPtr,
    /// context holding the rewritten tree, or NULL.
    pub query_context: MemoryContext,
    /// Role ID we did rewriting for.
    pub rewriteRoleId: Oid,
    /// `row_security` used during rewrite.
    pub rewriteRowSecurity: bool,
    /// is rewritten query specific to the above?
    pub dependsOnRLS: bool,
    /// generic plan, or NULL if not valid.
    pub gplan: *mut CachedPlan,
    /// is it a "oneshot" plan?
    pub is_oneshot: bool,
    /// has `CompleteCachedPlan` been done?
    pub is_complete: bool,
    /// has `CachedPlanSource` been "saved"?
    pub is_saved: bool,
    /// is the `query_list` currently valid?
    pub is_valid: bool,
    /// increments each time we create a plan.
    pub generation: i32,
    /// list link, if `is_saved`.
    pub node: dlist_node,
    /// cost of generic plan, or -1 if not known.
    pub generic_cost: f64,
    /// total cost of custom plans so far.
    pub total_custom_cost: f64,
    /// # of custom plans included in total.
    pub num_custom_plans: i64,
    /// # of generic plans.
    pub num_generic_plans: i64,
}

/// `CachedPlan` (plancache.h) — an execution plan derived from a source.
#[repr(C)]
pub struct CachedPlan {
    /// should equal `CACHEDPLAN_MAGIC`.
    pub magic: i32,
    /// list of `PlannedStmt`s.
    pub stmt_list: *mut List,
    /// is it a "oneshot" plan?
    pub is_oneshot: bool,
    /// is `CachedPlan` in a long-lived context?
    pub is_saved: bool,
    /// is the `stmt_list` currently valid?
    pub is_valid: bool,
    /// Role ID the plan was created for.
    pub planRoleId: Oid,
    /// is plan specific to that role?
    pub dependsOnRole: bool,
    /// replan when `TransactionXmin` changes from this.
    pub saved_xmin: TransactionId,
    /// parent's generation number for this plan.
    pub generation: i32,
    /// count of live references to this struct.
    pub refcount: i32,
    /// context containing this `CachedPlan`.
    pub context: MemoryContext,
}

/// `CachedExpression` (plancache.h) — cached planned scalar expression.
#[repr(C)]
pub struct CachedExpression {
    /// should equal `CACHEDEXPR_MAGIC`.
    pub magic: i32,
    /// planned form of expression (`Node *`).
    pub expr: NodePtr,
    /// is the expression still valid?
    pub is_valid: bool,
    /// OIDs of relations the expr depends on.
    pub relationOids: *mut List,
    /// other dependencies, as `PlanInvalItems`.
    pub invalItems: *mut List,
    /// context containing this `CachedExpression`.
    pub context: MemoryContext,
    /// link in global list of `CachedExpression`s.
    pub node: dlist_node,
}

/* ===========================================================================
 * relmapper.c — the shared/local relation map files.
 * ======================================================================== */

/// `RELMAPPER_FILEMAGIC` — version ID value.
pub const RELMAPPER_FILEMAGIC: i32 = 0x0059_2717;

/// `MAX_MAPPINGS` — capacity of a `RelMapFile`.
pub const MAX_MAPPINGS: usize = 64;

// `XLOG_RELMAP_UPDATE` and `xl_relmap_update` already live in `rmgrdesc`; the
// relmapper crate reaches them via the crate root.  Re-defining them here would
// trip the ambiguous-glob trap, so they are intentionally NOT duplicated.

/// `RelMapping` (relmapper.c) — one catalog-OID → filenumber entry.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelMapping {
    /// OID of a catalog.
    pub mapoid: Oid,
    /// its rel file number.
    pub mapfilenumber: RelFileNumber,
}

/// `RelMapFile` (relmapper.c) — the on-disk and in-shared-memory map file.
///
/// The shared instance of this struct lives in shared memory (it is registered
/// via `ShmemInitStruct` by relmapper); the layout here is the exact `repr(C)`
/// image that is read from / written to the `pg_*_relation_mapping` files and
/// copied into the shared segment.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelMapFile {
    /// always `RELMAPPER_FILEMAGIC`.
    pub magic: i32,
    /// number of valid `RelMapping` entries.
    pub num_mappings: i32,
    /// the mapping array.
    pub mappings: [RelMapping; MAX_MAPPINGS],
    /// CRC of all above.
    pub crc: crate::types::pg_crc32c,
}

/// `SerializedActiveRelMaps` (relmapper.c) — parallel-worker serialization.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SerializedActiveRelMaps {
    pub active_shared_updates: RelMapFile,
    pub active_local_updates: RelMapFile,
}

// `xl_relmap_update` (relmapper.h, the XLOG record header) already lives in
// `rmgrdesc` and is reachable from the relmapper crate via the crate root.

/* ===========================================================================
 * ts_cache.h — text-search caches.
 * ======================================================================== */

/// `TSAnyCacheEntry` (ts_cache.h) — common header of all TS cache entries.
#[repr(C)]
pub struct TSAnyCacheEntry {
    pub objId: Oid,
    pub isvalid: bool,
}

/// `TSParserCacheEntry` (ts_cache.h).
#[repr(C)]
pub struct TSParserCacheEntry {
    /// hash lookup key — MUST BE FIRST.
    pub prsId: Oid,
    pub isvalid: bool,
    pub startOid: Oid,
    pub tokenOid: Oid,
    pub endOid: Oid,
    pub headlineOid: Oid,
    pub lextypeOid: Oid,
    pub prsstart: FmgrInfo,
    pub prstoken: FmgrInfo,
    pub prsend: FmgrInfo,
    pub prsheadline: FmgrInfo,
}

/// `TSDictionaryCacheEntry` (ts_cache.h).
#[repr(C)]
pub struct TSDictionaryCacheEntry {
    /// hash lookup key — MUST BE FIRST.
    pub dictId: Oid,
    pub isvalid: bool,
    pub lexizeOid: Oid,
    pub lexize: FmgrInfo,
    /// memory context to store private data.
    pub dictCtx: MemoryContext,
    pub dictData: *mut c_void,
}

/// `ListDictionary` (ts_cache.h) — a config entry's per-token dictionary list.
#[repr(C)]
pub struct ListDictionary {
    pub len: i32,
    pub dictIds: *mut Oid,
}

/// `TSConfigCacheEntry` (ts_cache.h).
#[repr(C)]
pub struct TSConfigCacheEntry {
    /// hash lookup key — MUST BE FIRST.
    pub cfgId: Oid,
    pub isvalid: bool,
    pub prsId: Oid,
    pub lenmap: i32,
    pub map: *mut ListDictionary,
}

/* ===========================================================================
 * evtcache.h — event-trigger cache.
 * ======================================================================== */

/// `EventTriggerEvent` (evtcache.h).
pub type EventTriggerEvent = core::ffi::c_uint;
/// `EVT_DDLCommandStart`.
pub const EVT_DDL_COMMAND_START: EventTriggerEvent = 0;
/// `EVT_DDLCommandEnd`.
pub const EVT_DDL_COMMAND_END: EventTriggerEvent = 1;
/// `EVT_SQLDrop`.
pub const EVT_SQL_DROP: EventTriggerEvent = 2;
/// `EVT_TableRewrite`.
pub const EVT_TABLE_REWRITE: EventTriggerEvent = 3;
/// `EVT_Login`.
pub const EVT_LOGIN: EventTriggerEvent = 4;

/// `EventTriggerCacheItem` (evtcache.h) — one event trigger's cached data.
#[repr(C)]
pub struct EventTriggerCacheItem {
    /// function to be called.
    pub fnoid: Oid,
    /// as `SESSION_REPLICATION_ROLE_*`.
    pub enabled: c_char,
    /// command tags, or NULL if empty.
    pub tagset: *mut Bitmapset,
}

/// `EventTriggerCacheStateType` (evtcache.c) — rebuild state machine.
pub type EventTriggerCacheStateType = core::ffi::c_uint;
/// `ETCS_NEEDS_REBUILD`.
pub const ETCS_NEEDS_REBUILD: EventTriggerCacheStateType = 0;
/// `ETCS_REBUILD_STARTED`.
pub const ETCS_REBUILD_STARTED: EventTriggerCacheStateType = 1;
/// `ETCS_VALID`.
pub const ETCS_VALID: EventTriggerCacheStateType = 2;

/// `EventTriggerCacheEntry` (evtcache.c) — hash entry keyed by event.
#[repr(C)]
pub struct EventTriggerCacheEntry {
    pub event: EventTriggerEvent,
    /// list of `EventTriggerCacheItem *`.
    pub triggerlist: *mut List,
}

/* ===========================================================================
 * lsyscache.h — result structs / selectors.
 * ======================================================================== */

/// `OpIndexInterpretation` (lsyscache.h) — `get_op_index_interpretation` elem.
#[repr(C)]
pub struct OpIndexInterpretation {
    /// opfamily containing operator.
    pub opfamily_id: Oid,
    /// its generic comparison type.
    pub cmptype: CompareType,
    /// declared left input datatype.
    pub oplefttype: Oid,
    /// declared right input datatype.
    pub oprighttype: Oid,
}

/// `IOFuncSelector` (lsyscache.h) — selector for `get_type_io_data`.
pub type IOFuncSelector = core::ffi::c_uint;
/// `IOFunc_input`.
pub const IO_FUNC_INPUT: IOFuncSelector = 0;
/// `IOFunc_output`.
pub const IO_FUNC_OUTPUT: IOFuncSelector = 1;
/// `IOFunc_receive`.
pub const IO_FUNC_RECEIVE: IOFuncSelector = 2;
/// `IOFunc_send`.
pub const IO_FUNC_SEND: IOFuncSelector = 3;

/// `ATTSTATSSLOT_VALUES` — flag bit for `get_attstatsslot`.
pub const ATTSTATSSLOT_VALUES: i32 = 0x01;
/// `ATTSTATSSLOT_NUMBERS` — flag bit for `get_attstatsslot`.
pub const ATTSTATSSLOT_NUMBERS: i32 = 0x02;

/// `AttStatsSlot` (lsyscache.h) — result struct for `get_attstatsslot`.
#[repr(C)]
pub struct AttStatsSlot {
    /// actual staop for the found slot.
    pub staop: Oid,
    /// actual collation for the found slot.
    pub stacoll: Oid,
    /// actual datatype of the values.
    pub valuetype: Oid,
    /// slot's "values" array, or NULL if none.
    pub values: *mut Datum,
    /// length of `values[]`, or 0.
    pub nvalues: i32,
    /// slot's "numbers" array, or NULL if none (`float4 *`).
    pub numbers: *mut f32,
    /// length of `numbers[]`, or 0.
    pub nnumbers: i32,
    /// palloc'd values array, if any (private).
    pub values_arr: *mut c_void,
    /// palloc'd numbers array, if any (private).
    pub numbers_arr: *mut c_void,
}

/// `get_attavgwidth_hook_type` — `int32 (*)(Oid relid, AttrNumber attnum)`.
pub type GetAttavgwidthHookType =
    Option<unsafe extern "C" fn(relid: Oid, attnum: crate::types::AttrNumber) -> i32>;

// Re-export the `HeapTuple` alias through this module so downstream cache
// crates can name it via `cache_remainder::HeapTuple` (used by lsyscache's
// `getTypeIOParam`/`get_attstatsslot` signatures).
pub use crate::heaptuple::HeapTuple;
