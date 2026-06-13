//! Backend portal memory management (`utils/mmgr/portalmem.c`).
//!
//! Portals represent the execution state of a query. This module manages the
//! portal lifecycle and the per-backend `PortalHashTable`; it does not run the
//! executor (that is `tcop/pquery.c`).
//!
//! # Per-backend, not shared
//!
//! The `PortalHashTable` and `TopPortalContext` are per-backend process-local
//! state (C `static HTAB *` / `static MemoryContext`, no `HASH_SHARED_MEM`):
//! modeled here as `thread_local!` ([`PORTAL_HASH_TABLE`], [`TOP_PORTAL_CONTEXT`]),
//! keyed by a `MAX_PORTALNAME_LEN`-byte portal name like dynahash's
//! `HASH_STRINGS`.
//!
//! # Ownership
//!
//! The table owns each [`Portal`] record; a portal is named by its hash-table
//! key (the truncated name) — the idiomatic stand-in for the C `*mut PortalData`
//! handle. The portal's subsidiary memory arenas (`portalContext`,
//! `holdContext`) are real owned [`mcx::MemoryContext`] values; deleting one is
//! dropping it, `MemoryContextDeleteChildren` is `reset()`. Objects owned by
//! other subsystems portalmem only threads back to their owner (the resource
//! owner, the cached plan, the held tuplestore, snapshots, the cleanup hook,
//! the executor `QueryDesc`/params/env/`TupleDesc`/stmts) are identity tokens
//! routed through that owner's seam, panicking until the owner lands.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use backend_utils_error::{elog, ereport};
use mcx::{MemoryContext, PgString, PgVec};
use types_core::{SubTransactionId, TimestampTz};
use types_error::{
    PgResult, ERRCODE_DUPLICATE_CURSOR, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_CURSOR_STATE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, WARNING,
};
use types_portal::{
    CachedPlanHandle, CommandTag, ExternHandle, FcinfoHandle, PgCursorRow, PortalCleanupHook,
    PortalStatus, PortalStrategy, QueryCompletion, ResourceOwnerHandle, SnapshotHandle,
    CMDTAG_UNKNOWN, CURSOR_OPT_BINARY, CURSOR_OPT_HOLD, CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL,
    MAX_PORTALNAME_LEN, PORTAL_ACTIVE, PORTAL_DEFINED, PORTAL_DONE, PORTAL_FAILED,
    PORTAL_MULTI_QUERY, PORTAL_NEW, PORTAL_ONE_SELECT, PORTAL_READY, RESOURCE_RELEASE_AFTER_LOCKS,
    RESOURCE_RELEASE_BEFORE_LOCKS, RESOURCE_RELEASE_LOCKS,
};

use backend_access_transam_xact_seams as xact_seam;
use backend_commands_portalcmds_seams as portalcmds_seam;
use backend_utils_cache_plancache_portal_seams as plancache_seam;
use backend_utils_resowner_seams as resowner_seam;
use backend_utils_sort_tuplestore_hold_seams as tuplestore_seam;
use backend_utils_time_snapmgr_seams as snapmgr_seam;

pub mod seams_install;

/// `InvalidSubTransactionId` (`c.h`).
const InvalidSubTransactionId: SubTransactionId = 0;

/// Estimate of the maximum number of open portals a user would have, used to
/// size the `PortalHashTable` in [`EnablePortalManager`].
const PORTALS_PER_USER: usize = 16;

/// A handle naming a portal owned by the [`PortalHashTable`](PORTAL_HASH_TABLE):
/// the portal's truncated name (its hash-table key).
pub type PortalId = String;

/// `struct PortalData` (`utils/portal.h`). Fields portalmem reads/writes itself
/// are owned scalars/strings + owned `MemoryContext` arenas; fields naming
/// objects owned by other subsystems are identity tokens threaded through seams.
#[derive(Debug)]
pub struct Portal {
    // Bookkeeping
    pub name: String,
    pub prepStmtName: Option<String>,
    /// `portal->portalContext` — owned subsidiary arena (`None` == C NULL until
    /// `CreatePortal` assigns it).
    pub portalContext: Option<MemoryContext>,
    pub resowner: ResourceOwnerHandle,
    pub cleanup: PortalCleanupHook,

    // Subtransaction bookkeeping
    pub createSubid: SubTransactionId,
    pub activeSubid: SubTransactionId,
    pub createLevel: i32,

    // The query or queries the portal will execute
    pub sourceText: Option<String>,
    pub commandTag: CommandTag,
    pub qc: QueryCompletion,
    /// `portal->stmts` — planned-statement list, owned by the cached plan.
    pub stmts: ExternHandle,
    /// `portal->cplan` — `CachedPlan *` (`0` == NULL).
    pub cplan: CachedPlanHandle,

    pub portalParams: ExternHandle,
    pub queryEnv: ExternHandle,

    // Features/options
    pub strategy: PortalStrategy,
    pub cursorOptions: i32,

    // Status
    pub status: PortalStatus,
    pub portalPinned: bool,
    pub autoHeld: bool,

    // Executor invocation state
    pub queryDesc: ExternHandle,

    // Result presentation
    pub tupDesc: ExternHandle,
    pub formats: Vec<i16>,

    // Outermost ActiveSnapshot for execution
    pub portalSnapshot: SnapshotHandle,

    // Held-cursor tuple store
    pub holdStore: ExternHandle,
    /// `portal->holdContext` — owned arena for the hold store (`None` == C NULL).
    pub holdContext: Option<MemoryContext>,
    pub holdSnapshot: SnapshotHandle,

    // Cursor position
    pub atStart: bool,
    pub atEnd: bool,
    pub portalPos: u64,

    // Presentation
    pub creation_time: TimestampTz,
    pub visible: bool,
}

impl Portal {
    /// `MemoryContextAllocZero(TopPortalContext, sizeof *portal)`.
    fn zeroed() -> Portal {
        Portal {
            name: String::new(),
            prepStmtName: None,
            portalContext: None,
            resowner: ResourceOwnerHandle::NULL,
            cleanup: PortalCleanupHook::NONE,
            createSubid: 0,
            activeSubid: 0,
            createLevel: 0,
            sourceText: None,
            commandTag: CMDTAG_UNKNOWN,
            qc: QueryCompletion {
                commandTag: CMDTAG_UNKNOWN,
                nprocessed: 0,
            },
            stmts: ExternHandle::NONE,
            cplan: CachedPlanHandle::NULL,
            portalParams: ExternHandle::NONE,
            queryEnv: ExternHandle::NONE,
            strategy: PORTAL_MULTI_QUERY,
            cursorOptions: 0,
            status: PORTAL_NEW,
            portalPinned: false,
            autoHeld: false,
            queryDesc: ExternHandle::NONE,
            tupDesc: ExternHandle::NONE,
            formats: Vec::new(),
            portalSnapshot: SnapshotHandle::NULL,
            holdStore: ExternHandle::NONE,
            holdContext: None,
            holdSnapshot: SnapshotHandle::NULL,
            atStart: false,
            atEnd: false,
            portalPos: 0,
            creation_time: 0,
            visible: false,
        }
    }
}

thread_local! {
    /// `static HTAB *PortalHashTable = NULL;` — `None` == not yet enabled.
    static PORTAL_HASH_TABLE: RefCell<Option<HashMap<PortalId, Portal>>> =
        const { RefCell::new(None) };

    /// `static MemoryContext TopPortalContext = NULL;`
    static TOP_PORTAL_CONTEXT: RefCell<Option<MemoryContext>> = const { RefCell::new(None) };

    /// `CreateNewPortal`'s `static unsigned int unnamed_portal_count = 0;`.
    static UNNAMED_PORTAL_COUNT: RefCell<u32> = const { RefCell::new(0) };
}

/// dynahash `strlcpy(dest, src, MAX_PORTALNAME_LEN)` keying — names truncate to
/// `MAX_PORTALNAME_LEN - 1` bytes, so over-long names collide exactly as in the
/// backend.
fn hash_key(name: &str) -> PortalId {
    let max = MAX_PORTALNAME_LEN - 1;
    if name.len() <= max {
        name.to_owned()
    } else {
        let mut end = max;
        while end > 0 && !name.is_char_boundary(end) {
            end -= 1;
        }
        name[..end].to_owned()
    }
}

/// `name[0] ? name : "<unnamed>"`.
fn portal_name_or_unnamed(name: &str) -> &str {
    if name.is_empty() {
        "<unnamed>"
    } else {
        name
    }
}

// ===========================================================================
// Hash-table access (PortalHashTable{Lookup,Insert,Delete}) + scan snapshot.
// ===========================================================================

fn with_table<R>(f: impl FnOnce(Option<&mut HashMap<PortalId, Portal>>) -> R) -> R {
    PORTAL_HASH_TABLE.with(|tbl| f(tbl.borrow_mut().as_mut()))
}

/// Borrow the live portal named by `id`, run `f`; `None` if no such portal.
fn with_portal<R>(id: &str, f: impl FnOnce(&mut Portal) -> R) -> Option<R> {
    let key = hash_key(id);
    with_table(|t| t.and_then(|m| m.get_mut(&key)).map(f))
}

/// `PortalHashTableLookup` — `hash_search(HASH_FIND)`; the key if present.
fn portal_hash_table_lookup(name: &str) -> Option<PortalId> {
    let key = hash_key(name);
    with_table(|t| t.and_then(|m| m.contains_key(&key).then(|| key.clone())))
}

/// `PortalHashTableInsert` — `hash_search(HASH_ENTER, &found)`;
/// `elog(ERROR, "duplicate portal name")` on `found`; sets `portal->name` to
/// the canonical key copy. Returns the key.
fn portal_hash_table_insert(mut portal: Portal, name: &str) -> PgResult<PortalId> {
    let key = hash_key(name);
    PORTAL_HASH_TABLE.with(|tbl| {
        let mut tbl = tbl.borrow_mut();
        let m = tbl.as_mut().ok_or_else(|| {
            ereport(ERROR)
                .errmsg_internal("portal_hash_table_insert: PortalHashTable not enabled")
                .into_error()
        })?;
        match m.entry(key.clone()) {
            Entry::Occupied(_) => Err(ereport(ERROR)
                .errmsg_internal("duplicate portal name")
                .into_error()),
            Entry::Vacant(slot) => {
                portal.name = key.clone();
                slot.insert(portal);
                Ok(key)
            }
        }
    })
}

/// `PortalHashTableDelete` — `hash_search(HASH_REMOVE)`; `elog(WARNING)` if the
/// entry did not exist. Returns the removed [`Portal`] so the caller can run the
/// remaining teardown before it is dropped.
fn portal_hash_table_delete(id: &str) -> PgResult<Option<Portal>> {
    let key = hash_key(id);
    let removed = with_table(|t| t.and_then(|m| m.remove(&key)));
    if removed.is_none() {
        elog(WARNING, "trying to delete portal name that does not exist")?;
    }
    Ok(removed)
}

/// Snapshot every live portal's key — the basis of a `hash_seq_search` walk.
/// Callers that can drop portals re-snapshot each pass (C `hash_seq_term` +
/// `hash_seq_init`); pure read scans walk once. The transient key buffer (the C
/// backend's scan scratch in `CurrentMemoryContext`) is charged to a crate-owned
/// per-call working context that drops with the function, surfacing OOM as a
/// recoverable error rather than aborting.
fn portal_ids() -> PgResult<Vec<PortalId>> {
    let workspace = MemoryContext::new("portalmem.hash_seq");
    let mcx = workspace.mcx();

    let n = with_table(|t| t.map_or(0, |m| m.len()));
    let mut keys: PgVec<PgString> = mcx::vec_with_capacity_in(mcx, n)?;
    let raw: Vec<PortalId> = with_table(|t| t.map_or_else(Vec::new, |m| m.keys().cloned().collect()));
    for k in raw {
        keys.push(PgString::from_str_in(&k, mcx)?);
    }
    Ok(keys.iter().map(|s| s.as_str().to_owned()).collect())
}

// ===========================================================================
// public portal interface functions
// ===========================================================================

/// `EnablePortalManager` — enable the portal-management module at backend
/// startup (portalmem.c:103).
pub fn EnablePortalManager() -> PgResult<()> {
    debug_assert!(TOP_PORTAL_CONTEXT.with(|c| c.borrow().is_none()));

    // TopPortalContext = AllocSetContextCreate(TopMemoryContext, "TopPortalContext", ...)
    TOP_PORTAL_CONTEXT.with(|c| *c.borrow_mut() = Some(MemoryContext::new("TopPortalContext")));

    // hash_create("Portal hash", PORTALS_PER_USER, &ctl, HASH_ELEM | HASH_STRINGS)
    PORTAL_HASH_TABLE.with(|tbl| {
        *tbl.borrow_mut() = Some(HashMap::with_capacity(PORTALS_PER_USER));
    });
    Ok(())
}

/// `GetPortalByName` — a portal given its name, or `None` if not found
/// (portalmem.c:130).
pub fn GetPortalByName(name: Option<&str>) -> Option<PortalId> {
    match name {
        Some(name) => portal_hash_table_lookup(name),
        None => None,
    }
}

/// `PortalGetPrimaryStmt` — the "primary" stmt (the `canSetTag` one), or `NONE`
/// (portalmem.c:151). The `portal->stmts` list is owned by the cached plan, so
/// the `stmt->canSetTag` walk runs through the portalcmds seam.
pub fn PortalGetPrimaryStmt(portal: &str) -> ExternHandle {
    portalcmds_seam::first_can_set_tag_stmt::call(portal)
}

/// `CreatePortal` — a new portal given a name (portalmem.c:175).
///
/// `allowDup`: if true, automatically drop any pre-existing same-named portal
/// (else error). `dupSilent`: if true, don't even `WARNING`.
pub fn CreatePortal(name: &str, allowDup: bool, dupSilent: bool) -> PgResult<PortalId> {
    if let Some(existing) = GetPortalByName(Some(name)) {
        // PortalIsValid(portal)
        if !allowDup {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_CURSOR)
                .errmsg(format!("cursor \"{name}\" already exists"))
                .into_error());
        }
        if !dupSilent {
            ereport(WARNING)
                .errcode(ERRCODE_DUPLICATE_CURSOR)
                .errmsg(format!("closing existing cursor \"{name}\""))
                .finish(errloc(190, "CreatePortal"))?;
        }
        PortalDrop(&existing, false)?;
    }

    // make new portal structure
    let mut new_portal = Portal::zeroed();

    let portal_context = TOP_PORTAL_CONTEXT.with(|c| {
        c.borrow()
            .as_ref()
            .map(|top| top.new_child("PortalContext"))
    });
    let portal_context = portal_context.ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal(
                "CreatePortal: EnablePortalManager has not run (TopPortalContext is NULL)",
            )
            .into_error()
    })?;
    new_portal.portalContext = Some(portal_context);

    // create a resource owner for the portal
    new_portal.resowner = resowner_seam::resource_owner_create_portal::call();

    // initialize portal fields that don't start off zero
    new_portal.status = PORTAL_NEW;
    new_portal.cleanup = portalcmds_seam::portal_cleanup_hook::call();
    new_portal.createSubid = xact_seam::get_current_sub_transaction_id::call();
    new_portal.activeSubid = new_portal.createSubid;
    new_portal.createLevel = xact_seam::get_current_transaction_nest_level::call();
    new_portal.strategy = PORTAL_MULTI_QUERY;
    new_portal.cursorOptions = CURSOR_OPT_NO_SCROLL;
    new_portal.atStart = true;
    new_portal.atEnd = true; // disallow fetches until query is set
    new_portal.visible = true;
    new_portal.creation_time = xact_seam::get_current_statement_start_timestamp::call();

    // put portal in table (sets portal->name)
    let id = portal_hash_table_insert(new_portal, name)?;

    // for named portals reuse portal->name copy: MemoryContextSetIdentifier
    with_portal(&id, |p| {
        let ident = portal_name_or_unnamed(&p.name).to_owned();
        if let Some(ctx) = &p.portalContext {
            ctx.set_ident(Some(&ident));
        }
    })
    .ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal("CreatePortal: just-inserted portal not found")
            .into_error()
    })?;

    Ok(id)
}

/// `CreateNewPortal` — a new portal with a random nonconflicting name
/// (portalmem.c:235).
pub fn CreateNewPortal() -> PgResult<PortalId> {
    loop {
        let count = UNNAMED_PORTAL_COUNT.with(|c| {
            let mut c = c.borrow_mut();
            *c = c.wrapping_add(1);
            *c
        });
        let portalname = format!("<unnamed portal {count}>");
        if GetPortalByName(Some(&portalname)).is_none() {
            return CreatePortal(&portalname, false, false);
        }
    }
}

/// `PortalDefineQuery` — establish a portal's query (portalmem.c:282). Stores
/// the passed values; must not risk an error.
pub fn PortalDefineQuery(
    portal: &str,
    prepStmtName: Option<String>,
    sourceText: String,
    commandTag: CommandTag,
    stmts: ExternHandle,
    cplan: CachedPlanHandle,
) {
    let found = with_portal(portal, |p| {
        debug_assert_eq!(p.status, PORTAL_NEW);
        debug_assert!(commandTag != CMDTAG_UNKNOWN || stmts.is_none());

        p.prepStmtName = prepStmtName;
        p.sourceText = Some(sourceText);
        p.qc.commandTag = commandTag;
        p.qc.nprocessed = 0;
        p.commandTag = commandTag;
        p.stmts = stmts;
        p.cplan = cplan;
        p.status = PORTAL_DEFINED;
    });
    debug_assert!(found.is_some());
}

/// `PortalReleaseCachedPlan` — release a portal's cached-plan reference, if any
/// (portalmem.c:310).
fn PortalReleaseCachedPlan(portal: &mut Portal) {
    if !portal.cplan.is_null() {
        plancache_seam::release_cached_plan::call(portal.cplan);
        portal.cplan = CachedPlanHandle::NULL;
        // also clear portal->stmts which is now a dangling reference
        portal.stmts = ExternHandle::NONE;
    }
}

/// `PortalCreateHoldStore` — create the tuplestore for a portal
/// (portalmem.c:331).
pub fn PortalCreateHoldStore(portal: &str) -> PgResult<()> {
    let exists = TOP_PORTAL_CONTEXT.with(|c| c.borrow().is_some());
    if !exists {
        return Err(ereport(ERROR)
            .errmsg_internal("PortalCreateHoldStore: EnablePortalManager has not run")
            .into_error());
    }

    let (cursor_options, random_access) = with_portal(portal, |p| {
        debug_assert!(p.holdContext.is_none());
        debug_assert!(p.holdStore.is_none());
        debug_assert!(p.holdSnapshot.is_null());

        // Create the memory context used for storage of the tuple set. Note
        // this is NOT a child of the portal's portalContext: it is created
        // under TopPortalContext so it can outlive the source transaction.
        let hold_context = TOP_PORTAL_CONTEXT.with(|c| {
            c.borrow()
                .as_ref()
                .map(|top| top.new_child("PortalHoldContext"))
        });
        p.holdContext = hold_context;
        (p.cursorOptions, (p.cursorOptions & CURSOR_OPT_SCROLL) != 0)
    })
    .ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal(format!(
                "PortalCreateHoldStore: portal \"{portal}\" does not exist"
            ))
            .into_error()
    })?;
    let _ = cursor_options;

    // MemoryContextSwitchTo(holdContext): the C switches the current context so
    // tuplestore_begin_heap pallocs the store there. Here the store is created
    // by the tuplestore owner (seam); allocation targeting is the owner's
    // concern once it lands, so the switch is a no-op for portalmem.
    let store = tuplestore_seam::tuplestore_begin_heap::call(random_access);
    with_portal(portal, |p| p.holdStore = store);
    Ok(())
}

// ===========================================================================
// Live-field accessors (driver-facing). Each mirrors one C field read/write
// through the truncated-name key, asserting the portal exists where C
// dereferences a known-live Portal.
// ===========================================================================

pub fn portal_get_strategy(portal: &str) -> PortalStrategy {
    with_portal(portal, |p| p.strategy).expect("portal exists")
}
pub fn portal_set_strategy(portal: &str, strategy: PortalStrategy) {
    with_portal(portal, |p| p.strategy = strategy).expect("portal exists");
}
pub fn portal_get_at_start(portal: &str) -> bool {
    with_portal(portal, |p| p.atStart).expect("portal exists")
}
pub fn portal_set_at_start(portal: &str, atStart: bool) {
    with_portal(portal, |p| p.atStart = atStart).expect("portal exists");
}
pub fn portal_get_at_end(portal: &str) -> bool {
    with_portal(portal, |p| p.atEnd).expect("portal exists")
}
pub fn portal_set_at_end(portal: &str, atEnd: bool) {
    with_portal(portal, |p| p.atEnd = atEnd).expect("portal exists");
}
pub fn portal_get_portal_pos(portal: &str) -> u64 {
    with_portal(portal, |p| p.portalPos).expect("portal exists")
}
pub fn portal_set_portal_pos(portal: &str, portalPos: u64) {
    with_portal(portal, |p| p.portalPos = portalPos).expect("portal exists");
}
pub fn portal_get_tup_desc(portal: &str) -> ExternHandle {
    with_portal(portal, |p| p.tupDesc).expect("portal exists")
}
pub fn portal_set_tup_desc(portal: &str, tupDesc: ExternHandle) {
    with_portal(portal, |p| p.tupDesc = tupDesc).expect("portal exists");
}
pub fn portal_get_stmts(portal: &str) -> ExternHandle {
    with_portal(portal, |p| p.stmts).expect("portal exists")
}
pub fn portal_get_hold_store(portal: &str) -> ExternHandle {
    with_portal(portal, |p| p.holdStore).expect("portal exists")
}
pub fn portal_set_hold_store(portal: &str, holdStore: ExternHandle) {
    with_portal(portal, |p| p.holdStore = holdStore).expect("portal exists");
}
pub fn portal_get_query_desc(portal: &str) -> ExternHandle {
    with_portal(portal, |p| p.queryDesc).expect("portal exists")
}
pub fn portal_set_query_desc(portal: &str, queryDesc: ExternHandle) {
    with_portal(portal, |p| p.queryDesc = queryDesc).expect("portal exists");
}
pub fn portal_get_formats(portal: &str) -> Vec<i16> {
    with_portal(portal, |p| p.formats.clone()).expect("portal exists")
}
pub fn portal_set_formats(portal: &str, formats: Vec<i16>) {
    with_portal(portal, |p| p.formats = formats).expect("portal exists");
}
pub fn portal_get_qc(portal: &str) -> QueryCompletion {
    with_portal(portal, |p| p.qc).expect("portal exists")
}
pub fn portal_set_qc(portal: &str, qc: QueryCompletion) {
    with_portal(portal, |p| p.qc = qc).expect("portal exists");
}
pub fn portal_get_command_tag(portal: &str) -> CommandTag {
    with_portal(portal, |p| p.commandTag).expect("portal exists")
}
pub fn portal_set_command_tag(portal: &str, commandTag: CommandTag) {
    with_portal(portal, |p| p.commandTag = commandTag).expect("portal exists");
}
pub fn portal_get_cursor_options(portal: &str) -> i32 {
    with_portal(portal, |p| p.cursorOptions).expect("portal exists")
}
pub fn portal_set_cursor_options(portal: &str, cursorOptions: i32) {
    with_portal(portal, |p| p.cursorOptions = cursorOptions).expect("portal exists");
}
pub fn portal_get_create_subid(portal: &str) -> SubTransactionId {
    with_portal(portal, |p| p.createSubid).unwrap_or(InvalidSubTransactionId)
}
pub fn portal_get_cleanup(portal: &str) -> PortalCleanupHook {
    with_portal(portal, |p| p.cleanup).unwrap_or(PortalCleanupHook::NONE)
}
pub fn portal_get_status(portal: &str) -> PortalStatus {
    with_portal(portal, |p| p.status).expect("portal exists")
}
/// `portal->status = status` — raw write for the few driver paths that set
/// status directly (e.g. `PortalRun` resetting `PORTAL_READY`).
pub fn portal_set_status(portal: &str, status: PortalStatus) {
    with_portal(portal, |p| p.status = status).expect("portal exists");
}
pub fn portal_get_portal_snapshot(portal: &str) -> SnapshotHandle {
    with_portal(portal, |p| p.portalSnapshot).expect("portal exists")
}
pub fn portal_set_portal_snapshot(portal: &str, portalSnapshot: SnapshotHandle) {
    with_portal(portal, |p| p.portalSnapshot = portalSnapshot).expect("portal exists");
}
pub fn portal_get_hold_snapshot(portal: &str) -> SnapshotHandle {
    with_portal(portal, |p| p.holdSnapshot).expect("portal exists")
}
pub fn portal_set_hold_snapshot(portal: &str, holdSnapshot: SnapshotHandle) {
    with_portal(portal, |p| p.holdSnapshot = holdSnapshot).expect("portal exists");
}
pub fn portal_get_resowner(portal: &str) -> ResourceOwnerHandle {
    with_portal(portal, |p| p.resowner).expect("portal exists")
}
pub fn portal_set_resowner(portal: &str, resowner: ResourceOwnerHandle) {
    with_portal(portal, |p| p.resowner = resowner).expect("portal exists");
}
pub fn portal_get_portal_params(portal: &str) -> ExternHandle {
    with_portal(portal, |p| p.portalParams).expect("portal exists")
}
pub fn portal_set_portal_params(portal: &str, portalParams: ExternHandle) {
    with_portal(portal, |p| p.portalParams = portalParams).expect("portal exists");
}
pub fn portal_get_query_env(portal: &str) -> ExternHandle {
    with_portal(portal, |p| p.queryEnv).expect("portal exists")
}
pub fn portal_set_query_env(portal: &str, queryEnv: ExternHandle) {
    with_portal(portal, |p| p.queryEnv = queryEnv).expect("portal exists");
}
pub fn portal_get_cplan(portal: &str) -> CachedPlanHandle {
    with_portal(portal, |p| p.cplan).expect("portal exists")
}
pub fn portal_get_create_level(portal: &str) -> i32 {
    with_portal(portal, |p| p.createLevel).expect("portal exists")
}
pub fn portal_get_visible(portal: &str) -> bool {
    with_portal(portal, |p| p.visible).expect("portal exists")
}
/// `PortalSetVisible` analog — used by `exec_simple_query` to hide the unnamed
/// portal it runs each statement in.
pub fn PortalSetVisible(portal: &str, visible: bool) {
    with_portal(portal, |p| p.visible = visible).expect("portal exists");
}
pub fn portal_get_source_text(portal: &str) -> Option<String> {
    with_portal(portal, |p| p.sourceText.clone()).expect("portal exists")
}

/// `PinPortal` — protect a portal from dropping (portalmem.c:371).
pub fn PinPortal(portal: &str) -> PgResult<()> {
    let already = with_portal(portal, |p| {
        if p.portalPinned {
            true
        } else {
            p.portalPinned = true;
            false
        }
    })
    .ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal(format!("PinPortal: portal \"{portal}\" does not exist"))
            .into_error()
    })?;
    if already {
        return Err(ereport(ERROR)
            .errmsg_internal("portal already pinned")
            .into_error());
    }
    Ok(())
}

/// `UnpinPortal` (portalmem.c:380).
pub fn UnpinPortal(portal: &str) -> PgResult<()> {
    let not_pinned = with_portal(portal, |p| {
        if !p.portalPinned {
            true
        } else {
            p.portalPinned = false;
            false
        }
    })
    .ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal(format!("UnpinPortal: portal \"{portal}\" does not exist"))
            .into_error()
    })?;
    if not_pinned {
        return Err(ereport(ERROR)
            .errmsg_internal("portal not pinned")
            .into_error());
    }
    Ok(())
}

/// `MarkPortalActive` — READY → ACTIVE (portalmem.c:395).
pub fn MarkPortalActive(portal: &str) -> PgResult<()> {
    // For safety, this is a runtime test not just an Assert.
    let status = with_portal(portal, |p| p.status).ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal(format!("MarkPortalActive: portal \"{portal}\" does not exist"))
            .into_error()
    })?;
    if status != PORTAL_READY {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("portal \"{portal}\" cannot be run"))
            .into_error());
    }
    let subid = xact_seam::get_current_sub_transaction_id::call();
    with_portal(portal, |p| {
        p.status = PORTAL_ACTIVE;
        p.activeSubid = subid;
    });
    Ok(())
}

/// `MarkPortalDone` — ACTIVE → DONE (portalmem.c:414).
pub fn MarkPortalDone(portal: &str) -> PgResult<()> {
    let cleanup = with_portal(portal, |p| {
        debug_assert_eq!(p.status, PORTAL_ACTIVE);
        p.status = PORTAL_DONE;
        p.cleanup
    })
    .ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal(format!("MarkPortalDone: portal \"{portal}\" does not exist"))
            .into_error()
    })?;

    // Allow portalcmds.c to clean up the state it knows about.
    if cleanup.is_some() {
        portalcmds_seam::run_cleanup_hook::call(cleanup, portal)?;
        with_portal(portal, |p| p.cleanup = PortalCleanupHook::NONE);
    }
    Ok(())
}

/// `MarkPortalFailed` — → FAILED (portalmem.c:442).
pub fn MarkPortalFailed(portal: &str) -> PgResult<()> {
    let cleanup = with_portal(portal, |p| {
        debug_assert!(p.status != PORTAL_DONE);
        p.status = PORTAL_FAILED;
        p.cleanup
    })
    .ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal(format!("MarkPortalFailed: portal \"{portal}\" does not exist"))
            .into_error()
    })?;

    if cleanup.is_some() {
        portalcmds_seam::run_cleanup_hook::call(cleanup, portal)?;
        with_portal(portal, |p| p.cleanup = PortalCleanupHook::NONE);
    }
    Ok(())
}

/// `PortalDrop` — destroy the portal (portalmem.c:468).
pub fn PortalDrop(portal: &str, isTopCommit: bool) -> PgResult<()> {
    let (pinned, status, cleanup, portal_snapshot) =
        with_portal(portal, |p| (p.portalPinned, p.status, p.cleanup, p.portalSnapshot))
            .ok_or_else(|| {
                ereport(ERROR)
                    .errmsg_internal(format!("PortalDrop: portal \"{portal}\" does not exist"))
                    .into_error()
            })?;

    // Don't allow dropping a pinned portal.
    if pinned {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_CURSOR_STATE)
            .errmsg(format!("cannot drop pinned portal \"{portal}\""))
            .into_error());
    }

    // Not sure if the PORTAL_ACTIVE case can validly happen or not...
    if status == PORTAL_ACTIVE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_CURSOR_STATE)
            .errmsg(format!("cannot drop active portal \"{portal}\""))
            .into_error());
    }

    // Allow portalcmds.c to clean up the state it knows about.
    if cleanup.is_some() {
        portalcmds_seam::run_cleanup_hook::call(cleanup, portal)?;
        with_portal(portal, |p| p.cleanup = PortalCleanupHook::NONE);
    }

    // There shouldn't be an active snapshot anymore, except after error.
    debug_assert!(portal_snapshot.is_null() || !isTopCommit);

    // Remove portal from hash table. We capture the owning record so the
    // remaining teardown (which reads it) runs before it is freed.
    let mut p = match portal_hash_table_delete(portal)? {
        Some(p) => p,
        None => return Ok(()),
    };

    // drop cached plan reference, if any
    PortalReleaseCachedPlan(&mut p);

    // If portal has a snapshot protecting its data, release that.
    if !p.holdSnapshot.is_null() {
        if !p.resowner.is_null() {
            snapmgr_seam::unregister_snapshot_from_owner::call(p.holdSnapshot, p.resowner);
        }
        p.holdSnapshot = SnapshotHandle::NULL;
    }

    // Release any resources still attached to the portal.
    if !p.resowner.is_null() && (!isTopCommit || p.status == PORTAL_FAILED) {
        let is_commit = p.status != PORTAL_FAILED;

        resowner_seam::resource_owner_release::call(
            p.resowner,
            RESOURCE_RELEASE_BEFORE_LOCKS,
            is_commit,
            false,
        );
        resowner_seam::resource_owner_release::call(
            p.resowner,
            RESOURCE_RELEASE_LOCKS,
            is_commit,
            false,
        );
        resowner_seam::resource_owner_release::call(
            p.resowner,
            RESOURCE_RELEASE_AFTER_LOCKS,
            is_commit,
            false,
        );
        resowner_seam::resource_owner_delete::call(p.resowner);
    }
    p.resowner = ResourceOwnerHandle::NULL;

    // Delete tuplestore if present. We do this even under error conditions;
    // since the tuplestore uses cross-transaction storage, its temp files need
    // to be explicitly deleted.
    if p.holdStore.is_some() {
        if p.holdContext.is_none() {
            return Err(ereport(ERROR)
                .errmsg_internal("PortalDrop: holdStore set without holdContext")
                .into_error());
        }
        // MemoryContextSwitchTo(holdContext) — allocation targeting is the
        // tuplestore owner's concern; the end call frees the store + temp files.
        tuplestore_seam::tuplestore_end::call(p.holdStore);
        p.holdStore = ExternHandle::NONE;
    }

    // delete tuplestore storage, if any (drop the owned holdContext arena)
    p.holdContext = None;

    // release subsidiary storage (drop the owned portalContext arena)
    if p.portalContext.is_none() {
        return Err(ereport(ERROR)
            .errmsg_internal(
                "PortalDrop: portal has no portalContext (CreatePortal always assigns one)",
            )
            .into_error());
    }
    p.portalContext = None;

    // release portal struct (it's in TopPortalContext) — `p` drops here
    drop(p);
    Ok(())
}

/// Delete all declared cursors. Used by CLOSE ALL, DISCARD ALL (portalmem.c:607).
pub fn PortalHashTableDeleteAll() -> PgResult<()> {
    let enabled = PORTAL_HASH_TABLE.with(|tbl| tbl.borrow().is_some());
    if !enabled {
        return Ok(());
    }

    // hash_seq with restart-on-drop: re-snapshot after each PortalDrop.
    loop {
        let portals = portal_ids()?;
        let mut dropped_one = false;
        for portal in portals {
            // Can't close the active portal (the one running the command).
            let status = match with_portal(&portal, |p| p.status) {
                Some(s) => s,
                None => continue,
            };
            if status == PORTAL_ACTIVE {
                continue;
            }
            PortalDrop(&portal, false)?;
            dropped_one = true;
            break;
        }
        if !dropped_one {
            break;
        }
    }
    Ok(())
}

/// `HoldPortal` — prepare a portal for access by later transactions
/// (portalmem.c:636).
fn HoldPortal(portal: &str) -> PgResult<()> {
    // PersistHoldablePortal() must release all resources local to the creating
    // transaction.
    PortalCreateHoldStore(portal)?;
    portalcmds_seam::persist_holdable_portal::call(portal)?;

    // drop cached plan reference, if any
    with_portal(portal, PortalReleaseCachedPlan);

    with_portal(portal, |p| {
        // The portal will no longer have its own resources.
        p.resowner = ResourceOwnerHandle::NULL;
        // Mark it as not belonging to this transaction.
        p.createSubid = InvalidSubTransactionId;
        p.activeSubid = InvalidSubTransactionId;
        p.createLevel = 0;
    });
    Ok(())
}

/// `PreCommit_Portals` — pre-commit processing (portalmem.c:677). Returns true
/// if any portals changed state (possibly running user code).
pub fn PreCommit_Portals(isPrepare: bool) -> PgResult<bool> {
    let mut result = false;

    'restart: loop {
        let portals = portal_ids()?;
        for portal in portals {
            // The portal may have been dropped by an earlier iteration.
            let snap = match with_portal(&portal, |p| {
                (
                    p.portalPinned,
                    p.autoHeld,
                    p.status,
                    p.cursorOptions,
                    p.createSubid,
                    p.holdSnapshot,
                    p.resowner,
                )
            }) {
                Some(s) => s,
                None => continue,
            };
            let (pinned, auto_held, status, cursor_options, create_subid, hold_snapshot, resowner) =
                snap;

            // There should be no pinned portals anymore. Auto-held allowed.
            if pinned && !auto_held {
                return Err(ereport(ERROR)
                    .errmsg_internal("cannot commit while a portal is pinned")
                    .into_error());
            }

            // Do not touch active portals.
            if status == PORTAL_ACTIVE {
                if !hold_snapshot.is_null() {
                    if !resowner.is_null() {
                        snapmgr_seam::unregister_snapshot_from_owner::call(hold_snapshot, resowner);
                    }
                    with_portal(&portal, |p| p.holdSnapshot = SnapshotHandle::NULL);
                }
                with_portal(&portal, |p| {
                    p.resowner = ResourceOwnerHandle::NULL;
                    // Clear portalSnapshot too, for cleanliness.
                    p.portalSnapshot = SnapshotHandle::NULL;
                });
                continue;
            }

            // Is it a holdable portal created in the current xact?
            if (cursor_options & CURSOR_OPT_HOLD) != 0
                && create_subid != InvalidSubTransactionId
                && status == PORTAL_READY
            {
                if isPrepare {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("cannot PREPARE a transaction that has created a cursor WITH HOLD")
                        .into_error());
                }

                HoldPortal(&portal)?;
                result = true;
            } else if create_subid == InvalidSubTransactionId {
                // Do nothing to cursors held over from a previous transaction.
                continue;
            } else {
                // Zap all non-holdable portals.
                PortalDrop(&portal, true)?;
                result = true;
            }

            // After freezing or dropping a portal, restart the iteration.
            continue 'restart;
        }
        break;
    }

    Ok(result)
}

/// `AtAbort_Portals` (portalmem.c:781).
pub fn AtAbort_Portals() -> PgResult<()> {
    let portals = portal_ids()?;
    for portal in portals {
        let snap = match with_portal(&portal, |p| (p.status, p.createSubid, p.autoHeld)) {
            Some(s) => s,
            None => continue,
        };
        let (mut status, create_subid, auto_held) = snap;

        // When elog(FATAL) is in progress, set the active portal to failed.
        if status == PORTAL_ACTIVE
            && backend_storage_ipc_dsm_core::ipc::shmem_exit_inprogress()
        {
            MarkPortalFailed(&portal)?;
            status = with_portal(&portal, |p| p.status).unwrap_or(status);
        }

        // Do nothing else to cursors held over from a previous transaction.
        if create_subid == InvalidSubTransactionId {
            continue;
        }

        // Do nothing to auto-held cursors.
        if auto_held {
            continue;
        }

        // Created in the current transaction: can't do normal shutdown on a
        // READY portal either.
        if status == PORTAL_READY {
            MarkPortalFailed(&portal)?;
        }

        // Allow portalcmds.c to clean up, if we haven't already.
        let cleanup = with_portal(&portal, |p| p.cleanup).unwrap_or(PortalCleanupHook::NONE);
        if cleanup.is_some() {
            portalcmds_seam::run_cleanup_hook::call(cleanup, &portal)?;
            with_portal(&portal, |p| p.cleanup = PortalCleanupHook::NONE);
        }

        // drop cached plan reference, if any
        with_portal(&portal, PortalReleaseCachedPlan);

        // Resources will be released in the upcoming transaction-wide cleanup.
        let (status, _) = with_portal(&portal, |p| {
            p.resowner = ResourceOwnerHandle::NULL;
            (p.status, ())
        })
        .unwrap_or((status, ()));

        // Release any memory in subsidiary contexts, but leave active alone.
        if status != PORTAL_ACTIVE {
            with_portal(&portal, |p| {
                if let Some(ctx) = &mut p.portalContext {
                    ctx.reset();
                }
            });
        }
    }
    Ok(())
}

/// `AtCleanup_Portals` (portalmem.c:858). Single scan, no restart (no user code
/// runs here).
pub fn AtCleanup_Portals() -> PgResult<()> {
    let portals = portal_ids()?;
    for portal in portals {
        let snap =
            match with_portal(&portal, |p| (p.status, p.createSubid, p.autoHeld, p.resowner)) {
                Some(s) => s,
                None => continue,
            };
        let (status, create_subid, auto_held, resowner) = snap;

        // Do not touch active portals.
        if status == PORTAL_ACTIVE {
            continue;
        }

        // Do nothing to cursors held over from a previous transaction or
        // auto-held ones.
        if create_subid == InvalidSubTransactionId || auto_held {
            debug_assert!(status != PORTAL_ACTIVE);
            debug_assert!(resowner.is_null());
            continue;
        }

        // If a portal is still pinned, forcibly unpin it.
        with_portal(&portal, |p| {
            if p.portalPinned {
                p.portalPinned = false;
            }
        });

        // We had better not call any user-defined code during cleanup; if the
        // cleanup hook hasn't been run yet, skip it.
        let cleanup = with_portal(&portal, |p| p.cleanup).unwrap_or(PortalCleanupHook::NONE);
        if cleanup.is_some() {
            elog(WARNING, format!("skipping cleanup for portal \"{portal}\""))?;
            with_portal(&portal, |p| p.cleanup = PortalCleanupHook::NONE);
        }

        // Zap it.
        PortalDrop(&portal, false)?;
    }
    Ok(())
}

/// `PortalErrorCleanup` — portal cleanup when returning to the main loop on
/// error (portalmem.c:917). Single scan, no restart.
pub fn PortalErrorCleanup() -> PgResult<()> {
    let portals = portal_ids()?;
    for portal in portals {
        let auto_held = match with_portal(&portal, |p| p.autoHeld) {
            Some(a) => a,
            None => continue,
        };
        if auto_held {
            with_portal(&portal, |p| p.portalPinned = false);
            PortalDrop(&portal, false)?;
        }
    }
    Ok(())
}

/// `AtSubCommit_Portals` (portalmem.c:943).
pub fn AtSubCommit_Portals(
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
    parentLevel: i32,
    parentXactOwner: ResourceOwnerHandle,
) -> PgResult<()> {
    let portals = portal_ids()?;
    for portal in portals {
        let owner = with_portal(&portal, |p| {
            let was_mine = p.createSubid == mySubid;
            if was_mine {
                p.createSubid = parentSubid;
                p.createLevel = parentLevel;
            }
            if p.activeSubid == mySubid {
                p.activeSubid = parentSubid;
            }
            if was_mine && !p.resowner.is_null() {
                Some(p.resowner)
            } else {
                None
            }
        })
        .flatten();
        if let Some(owner) = owner {
            resowner_seam::resource_owner_new_parent::call(owner, parentXactOwner);
        }
    }
    Ok(())
}

/// `AtSubAbort_Portals` (portalmem.c:979).
pub fn AtSubAbort_Portals(
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
    myXactOwner: ResourceOwnerHandle,
    parentXactOwner: ResourceOwnerHandle,
) -> PgResult<()> {
    let _ = parentXactOwner;

    let portals = portal_ids()?;
    for portal in portals {
        let create_subid = match with_portal(&portal, |p| p.createSubid) {
            Some(c) => c,
            None => continue,
        };

        // Was it created in this subtransaction?
        if create_subid != mySubid {
            // No, but maybe it was used in this subtransaction?
            let active_subid = with_portal(&portal, |p| p.activeSubid).unwrap_or(0);
            if active_subid == mySubid {
                // Maintain activeSubid until the portal is removed.
                with_portal(&portal, |p| p.activeSubid = parentSubid);

                // Force a left-ACTIVE upper-level portal into FAILED state.
                let status = with_portal(&portal, |p| p.status).unwrap_or(PORTAL_FAILED);
                if status == PORTAL_ACTIVE {
                    MarkPortalFailed(&portal)?;
                }

                // If we failed it during the current subtransaction, reattach
                // its resource owner to the current subxact's owner.
                let owner = with_portal(&portal, |p| {
                    if p.status == PORTAL_FAILED && !p.resowner.is_null() {
                        let o = p.resowner;
                        p.resowner = ResourceOwnerHandle::NULL;
                        Some(o)
                    } else {
                        None
                    }
                })
                .flatten();
                if let Some(owner) = owner {
                    resowner_seam::resource_owner_new_parent::call(owner, myXactOwner);
                }
            }
            // Done if it wasn't created in this subtransaction.
            continue;
        }

        // Force any live portals of my own subtransaction into FAILED state.
        let status = with_portal(&portal, |p| p.status).unwrap_or(PORTAL_FAILED);
        if status == PORTAL_READY || status == PORTAL_ACTIVE {
            MarkPortalFailed(&portal)?;
        }

        // Allow portalcmds.c to clean up, if we haven't already.
        let cleanup = with_portal(&portal, |p| p.cleanup).unwrap_or(PortalCleanupHook::NONE);
        if cleanup.is_some() {
            portalcmds_seam::run_cleanup_hook::call(cleanup, &portal)?;
            with_portal(&portal, |p| p.cleanup = PortalCleanupHook::NONE);
        }

        // drop cached plan reference, if any
        with_portal(&portal, PortalReleaseCachedPlan);

        // Resources will be released in the upcoming transaction-wide cleanup.
        with_portal(&portal, |p| {
            p.resowner = ResourceOwnerHandle::NULL;
            // Release any memory in subsidiary contexts, such as executor state.
            if let Some(ctx) = &mut p.portalContext {
                ctx.reset();
            }
        });
    }
    Ok(())
}

/// `AtSubCleanup_Portals` (portalmem.c:1092). Single scan, no restart.
pub fn AtSubCleanup_Portals(mySubid: SubTransactionId) -> PgResult<()> {
    let portals = portal_ids()?;
    for portal in portals {
        let create_subid = match with_portal(&portal, |p| p.createSubid) {
            Some(c) => c,
            None => continue,
        };
        if create_subid != mySubid {
            continue;
        }

        // If a portal is still pinned, forcibly unpin it.
        with_portal(&portal, |p| {
            if p.portalPinned {
                p.portalPinned = false;
            }
        });

        // We had better not call any user-defined code during cleanup.
        let cleanup = with_portal(&portal, |p| p.cleanup).unwrap_or(PortalCleanupHook::NONE);
        if cleanup.is_some() {
            elog(WARNING, format!("skipping cleanup for portal \"{portal}\""))?;
            with_portal(&portal, |p| p.cleanup = PortalCleanupHook::NONE);
        }

        // Zap it.
        PortalDrop(&portal, false)?;
    }
    Ok(())
}

/// `pg_cursor(PG_FUNCTION_ARGS)` — find all available cursors (portalmem.c:1131).
///
/// The in-crate part is the one-scan `hash_seq_search` walk collecting every
/// visible, defined (`sourceText` set) portal. The SRF / `Datum` body
/// (`InitMaterializedSRF` + per-row `Datum` conversions + `tuplestore_putvalues`)
/// is the fmgr/`Datum` value layer (project-wide deferral) and routes through
/// the portalcmds seam.
pub fn pg_cursor(fcinfo: FcinfoHandle) -> PgResult<types_datum::Datum> {
    let ids = portal_ids()?;
    let workspace = MemoryContext::new("pg_cursor");
    let mcx = workspace.mcx();

    let mut rows: PgVec<PgCursorRow> = mcx::vec_with_capacity_in(mcx, ids.len())?;
    for portal in &ids {
        let snap = with_portal(portal, |p| {
            // report only "visible" entries
            if !p.visible {
                return None;
            }
            // ignore it if PortalDefineQuery hasn't been called yet
            let statement = p.sourceText.as_ref()?.clone();
            Some(PgCursorRow {
                name: p.name.clone(),
                statement,
                is_holdable: (p.cursorOptions & CURSOR_OPT_HOLD) != 0,
                is_binary: (p.cursorOptions & CURSOR_OPT_BINARY) != 0,
                is_scrollable: (p.cursorOptions & CURSOR_OPT_SCROLL) != 0,
                creation_time: p.creation_time,
            })
        })
        .flatten();
        if let Some(row) = snap {
            rows.push(row);
        }
    }

    portalcmds_seam::pg_cursor_srf::call(fcinfo, &rows)
}

/// `ThereAreNoReadyPortals` (portalmem.c:1171).
pub fn ThereAreNoReadyPortals() -> PgResult<bool> {
    let portals = portal_ids()?;
    for portal in portals {
        if with_portal(&portal, |p| p.status) == Some(PORTAL_READY) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `HoldPinnedPortals` — hold all pinned portals (portalmem.c:1207).
pub fn HoldPinnedPortals() -> PgResult<()> {
    let portals = portal_ids()?;
    for portal in portals {
        let snap =
            match with_portal(&portal, |p| (p.portalPinned, p.autoHeld, p.strategy, p.status)) {
                Some(s) => s,
                None => continue,
            };
        let (pinned, auto_held, strategy, status) = snap;
        if pinned && !auto_held {
            // Transaction control inside a non-read-only cursor loop has weird
            // semantics; such portals cannot be held.
            if strategy != PORTAL_ONE_SELECT {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(
                        "cannot perform transaction commands inside a cursor loop that is not read-only",
                    )
                    .into_error());
            }

            // Verify it's in a suitable state to be held.
            if status != PORTAL_READY {
                return Err(ereport(ERROR)
                    .errmsg_internal("pinned portal is not ready to be auto-held")
                    .into_error());
            }

            HoldPortal(&portal)?;
            with_portal(&portal, |p| p.autoHeld = true);
        }
    }
    Ok(())
}

/// `ForgetPortalSnapshots` — drop the outer active snapshots for all portals
/// (portalmem.c:1256).
pub fn ForgetPortalSnapshots() -> PgResult<()> {
    let mut numPortalSnaps: i32 = 0;
    let mut numActiveSnaps: i32 = 0;

    // First, scan PortalHashTable and clear portalSnapshot fields.
    let portals = portal_ids()?;
    for portal in portals {
        with_portal(&portal, |p| {
            if !p.portalSnapshot.is_null() {
                p.portalSnapshot = SnapshotHandle::NULL;
                numPortalSnaps += 1;
            }
            // portal->holdSnapshot will be cleaned up in PreCommit_Portals.
        });
    }

    // Now, pop all the active snapshots.
    while snapmgr_seam::active_snapshot_set::call() {
        snapmgr_seam::pop_active_snapshot::call();
        numActiveSnaps += 1;
    }

    if numPortalSnaps != numActiveSnaps {
        return Err(ereport(ERROR)
            .errmsg_internal(format!(
                "portal snapshots ({numPortalSnaps}) did not account for all active snapshots ({numActiveSnaps})"
            ))
            .into_error());
    }
    Ok(())
}

/// `ErrorLocation` for an `ereport(...).finish(...)` site
/// (`__FILE__`/`__LINE__`/`__func__`).
fn errloc(lineno: i32, funcname: &'static str) -> types_error::ErrorLocation {
    types_error::ErrorLocation::new("portalmem.c", lineno, funcname)
}

pub use seams_install::init_seams;

#[cfg(test)]
mod tests;
