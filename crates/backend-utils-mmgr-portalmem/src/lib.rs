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
//! The table owns each [`Portal`](types_portal::Portal) record (the
//! `Rc<RefCell<PortalData>>` open handle — the same shared, interior-mutable
//! alias the whole portal subsystem uses, cf. `types-rel`'s `Relation`). The
//! portal's subsidiary memory arenas (`portalContext`, `holdContext`) are real
//! owned [`mcx::MemoryContext`] values; deleting one is dropping it,
//! `MemoryContextDeleteChildren` is `reset()`. The planned-statement list
//! (`stmts`) is the real owned `Vec<PlannedStmt>`. Objects owned by other
//! subsystems (the resource owner, the cached plan, snapshots, the cleanup
//! hook) portalmem only threads back to their owner through that owner's seam.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use backend_utils_error::{elog, ereport};
use mcx::{MemoryContext, PgString, PgVec};
use types_core::SubTransactionId;
use types_error::{
    PgResult, ERRCODE_DUPLICATE_CURSOR, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_CURSOR_STATE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, WARNING,
};
use types_nodes::nodeindexscan::PlannedStmt;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_portal::{
    CachedPlanHandle, CommandTag, FcinfoHandle, PgCursorRow, Portal, PortalCleanupHook, PortalData,
    PortalStatus, PortalStrategy, QueryCompletion, ResourceOwner, CMDTAG_SELECT, CMDTAG_UNKNOWN,
    CURSOR_OPT_BINARY, CURSOR_OPT_HOLD, CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL, MAX_PORTALNAME_LEN,
    PORTAL_ACTIVE, PORTAL_DEFINED, PORTAL_DONE, PORTAL_FAILED, PORTAL_MULTI_QUERY, PORTAL_NEW,
    PORTAL_ONE_SELECT, PORTAL_READY, RESOURCE_RELEASE_AFTER_LOCKS, RESOURCE_RELEASE_BEFORE_LOCKS,
    RESOURCE_RELEASE_LOCKS,
};

use backend_access_transam_xact_seams as xact_seam;
use backend_commands_portalcmds_seams as portalcmds_seam;
use backend_utils_cache_plancache_portal_seams as plancache_seam;
use backend_utils_resowner_seams as resowner_seam;
use backend_utils_sort_tuplestore_hold_seams as tuplestore_seam;
use backend_utils_time_snapmgr_seams as snapmgr_seam;

pub mod seams_install;
pub mod top_context;

/// `InvalidSubTransactionId` (`c.h`).
const InvalidSubTransactionId: SubTransactionId = 0;

/// Estimate of the maximum number of open portals a user would have, used to
/// size the `PortalHashTable` in [`EnablePortalManager`].
const PORTALS_PER_USER: usize = 16;

/// A handle naming a portal owned by the [`PortalHashTable`](PORTAL_HASH_TABLE):
/// the portal's truncated name (its hash-table key).
pub type PortalId = String;

/// `MemoryContextAllocZero(TopPortalContext, sizeof(PortalData))` — a freshly
/// zeroed `PortalData`.
fn zeroed_portal_data() -> PortalData {
    PortalData {
        name: String::new(),
        prepStmtName: None,
        portalContext: None,
        resowner: ResourceOwner::default(),
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
        stmts: None,
        cplan: CachedPlanHandle::NULL,
        portalParams: None,
        queryEnv: None,
        strategy: PORTAL_MULTI_QUERY,
        cursorOptions: 0,
        status: PORTAL_NEW,
        portalPinned: false,
        autoHeld: false,
        queryDesc: None,
        tupDesc: None,
        formats: Vec::new(),
        portalSnapshot: None,
        holdStore: None,
        holdContext: None,
        holdSnapshot: None,
        atStart: false,
        atEnd: false,
        portalPos: 0,
        creation_time: 0,
        visible: false,
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

    /// `Portal ActivePortal = NULL;` (`utils/mmgr/portalmem.c` per-backend
    /// global) — the portal currently being run. Saved/set/restored around the
    /// `PersistHoldablePortal` PG_TRY in portalcmds via [`with_portal_globals`].
    static ACTIVE_PORTAL: RefCell<Option<Portal>> = const { RefCell::new(None) };

    /// `MemoryContext PortalContext = NULL;` (`utils/mmgr/mcxt.c` per-backend
    /// global) — the active portal's context. The handle is the portal whose
    /// `portalContext` is current.
    static PORTAL_CONTEXT_OWNER: RefCell<Option<Portal>> = const { RefCell::new(None) };
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

/// Look up the [`Portal`] open handle named by `id` (the truncated key); `None`
/// if no such portal. Returns the shared handle so the caller can `borrow()` /
/// `borrow_mut()` the live `PortalData` and thread it across seams.
fn lookup_portal(id: &str) -> Option<Portal> {
    let key = hash_key(id);
    with_table(|t| t.and_then(|m| m.get(&key).cloned()))
}

/// `PortalHashTableLookup` — `hash_search(HASH_FIND)`; the key if present.
fn portal_hash_table_lookup(name: &str) -> Option<PortalId> {
    let key = hash_key(name);
    with_table(|t| t.and_then(|m| m.contains_key(&key).then(|| key.clone())))
}

/// `PortalHashTableInsert` — `hash_search(HASH_ENTER, &found)`;
/// `elog(ERROR, "duplicate portal name")` on `found`; sets `portal->name` to
/// the canonical key copy. Returns the inserted [`Portal`] handle.
fn portal_hash_table_insert(portal: Portal, name: &str) -> PgResult<Portal> {
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
                portal.borrow_mut().name = key.clone();
                slot.insert(portal.clone());
                Ok(portal)
            }
        }
    })
}

/// `PortalHashTableDelete` — `hash_search(HASH_REMOVE)`; `elog(WARNING)` if the
/// entry did not exist. Returns the removed [`Portal`] handle so the caller can
/// run the remaining teardown before it is dropped.
fn portal_hash_table_delete(portal: &Portal) -> PgResult<()> {
    let key = hash_key(&portal.borrow().name);
    let removed = with_table(|t| t.and_then(|m| m.remove(&key)));
    if removed.is_none() {
        elog(WARNING, "trying to delete portal name that does not exist")?;
    }
    Ok(())
}

/// Snapshot every live portal handle — the basis of a `hash_seq_search` walk.
/// Callers that can drop portals re-snapshot each pass (C `hash_seq_term` +
/// `hash_seq_init`); pure read scans walk once. The transient key buffer (the C
/// backend's scan scratch in `CurrentMemoryContext`) is charged to a crate-owned
/// per-call working context that drops with the function, surfacing OOM as a
/// recoverable error rather than aborting.
fn portal_handles() -> PgResult<Vec<Portal>> {
    let workspace = MemoryContext::new("portalmem.hash_seq");
    let mcx = workspace.mcx();

    let n = with_table(|t| t.map_or(0, |m| m.len()));
    // Charge the scratch sizing to the per-call context (mirrors the C scan
    // scratch in CurrentMemoryContext), surfacing OOM as a recoverable error.
    let mut keys: PgVec<PgString> = mcx::vec_with_capacity_in(mcx, n)?;
    let raw: Vec<Portal> = with_table(|t| t.map_or_else(Vec::new, |m| m.values().cloned().collect()));
    for p in &raw {
        keys.push(PgString::from_str_in(&p.borrow().name, mcx)?);
    }
    Ok(raw)
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
pub fn GetPortalByName(name: Option<&str>) -> Option<Portal> {
    match name {
        Some(name) => portal_hash_table_lookup(name).and_then(|id| lookup_portal(&id)),
        None => None,
    }
}

/// `PortalGetPrimaryStmt` — get the "primary" stmt within a portal, i.e., the
/// one marked `canSetTag` (portalmem.c:151).
///
/// The C returns the `PlannedStmt *` it finds (a pointer into `portal->stmts`);
/// the in-crate `portal->stmts` is an owned `Vec<PlannedStmt>`, so this returns
/// the index of the first `canSetTag` stmt (the positional analog of that
/// pointer), or `None` if there is none. The `foreach`/`canSetTag` walk is
/// portalmem logic over the real `PlannedStmt` and runs here, not across a
/// seam.
pub fn PortalGetPrimaryStmt(portal: &Portal) -> Option<usize> {
    let data = portal.borrow();
    let stmts = data.stmts.as_ref()?;
    for (i, stmt) in stmts.iter().enumerate() {
        if stmt.canSetTag {
            return Some(i);
        }
    }
    None
}

/// `CreatePortal` — a new portal given a name (portalmem.c:175).
///
/// `allowDup`: if true, automatically drop any pre-existing same-named portal
/// (else error). `dupSilent`: if true, don't even `WARNING`.
pub fn CreatePortal(name: &str, allowDup: bool, dupSilent: bool) -> PgResult<Portal> {
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
    let mut data = zeroed_portal_data();

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
    data.portalContext = Some(portal_context);

    // create a resource owner for the portal
    data.resowner = resowner_seam::resource_owner_create_portal::call();

    // initialize portal fields that don't start off zero
    data.status = PORTAL_NEW;
    data.cleanup = portalcmds_seam_cleanup_hook();
    data.createSubid = xact_seam::get_current_sub_transaction_id::call();
    data.activeSubid = data.createSubid;
    data.createLevel = xact_seam::get_current_transaction_nest_level::call();
    data.strategy = PORTAL_MULTI_QUERY;
    data.cursorOptions = CURSOR_OPT_NO_SCROLL;
    data.atStart = true;
    data.atEnd = true; // disallow fetches until query is set
    data.visible = true;
    data.creation_time = xact_seam::get_current_statement_start_timestamp::call();

    // put portal in table (sets portal->name)
    let portal = portal_hash_table_insert(Portal::new(data), name)?;

    // for named portals reuse portal->name copy: MemoryContextSetIdentifier
    {
        let p = portal.borrow();
        let ident = portal_name_or_unnamed(&p.name).to_owned();
        if let Some(ctx) = &p.portalContext {
            ctx.set_ident(Some(&ident));
        }
    }

    Ok(portal)
}

/// The `PortalCleanup` function pointer `CreatePortal` installs as
/// `portal->cleanup` (`portalcmds.c`). portalcmds owns the hook; portalmem only
/// records that it is present (`PortalCleanupHook`) and later invokes
/// `portalcmds_seam::portal_cleanup`. The C `portal->cleanup = PortalCleanup`
/// is modeled by a non-NONE marker; the actual call routes through the seam.
fn portalcmds_seam_cleanup_hook() -> PortalCleanupHook {
    // Marker: cleanup hook is installed (PortalCleanup). The real call site is
    // run_portal_cleanup_hook, which dispatches portalcmds_seam::portal_cleanup.
    PortalCleanupHook(1)
}

/// Run `portal->cleanup(portal)` if set, clearing it afterwards
/// (`portal->cleanup = NULL`). The hook is portalcmds' `PortalCleanup`; it shuts
/// the executor down and may run user code.
fn run_portal_cleanup_hook(portal: &Portal) -> PgResult<()> {
    let has_cleanup = portal.borrow().cleanup.is_some();
    if has_cleanup {
        portalcmds_seam::portal_cleanup::call(portal.clone())?;
        portal.borrow_mut().cleanup = PortalCleanupHook::NONE;
    }
    Ok(())
}

/// `CreateNewPortal` — a new portal with a random nonconflicting name
/// (portalmem.c:235).
pub fn CreateNewPortal() -> PgResult<Portal> {
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
    portal: &Portal,
    prepStmtName: Option<String>,
    sourceText: String,
    commandTag: CommandTag,
    stmts: Option<Vec<PlannedStmt<'static>>>,
    cplan: CachedPlanHandle,
) {
    let mut p = portal.borrow_mut();
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
}

/// Deep-copy `stmts` (working-context `PlannedStmt`s) into the portal's own
/// `portalContext` arena, returning them carrying the portal-context-lived
/// `'static` marker.
///
/// portalmem owns `portal->portalContext`; this is C's
/// `MemoryContextSwitchTo(portal->portalContext); stmts = copyObject(stmts)`.
/// Each copy is allocated in `portalContext` via [`PlannedStmt::clone_in`] (real
/// `Global`-heap memory owned by the copy's inner `PgBox`/`PgVec`, accounted to
/// that context). The descriptor is freed when the `stmts` field is dropped —
/// which [`PortalData`]'s `Drop` and [`PortalDrop`] guarantee happens *before*
/// `portalContext` — so extending the clone's lifetime to the field's `'static`
/// marker is sound, exactly as [`portal_set_tup_desc`]/[`set_result_tup_desc_with`]
/// argue for `tupDesc`.
fn copy_stmts_into_portal_context(
    portal: &Portal,
    stmts: &[PlannedStmt<'_>],
) -> PgResult<Vec<PlannedStmt<'static>>> {
    let p = portal.borrow();
    let ctx = p.portalContext.as_ref().ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal("copy_stmts_into_portal_context: portal has no portalContext")
            .into_error()
    })?;
    let mcx = ctx.mcx();
    let mut out: Vec<PlannedStmt<'static>> = Vec::with_capacity(stmts.len());
    for stmt in stmts {
        let copied = stmt.clone_in(mcx)?;
        // SAFETY: `copied` lives in `portalContext` (real owned heap, freed by
        // its own `PgBox`/`PgVec` `Drop`). The `stmts` field is dropped before
        // `portalContext` (PortalData::drop + PortalDrop ordering), so the copy
        // never outlives the arena it deallocates through. The `'static` is the
        // portal-context-lived marker, like `tupDesc`.
        out.push(unsafe {
            core::mem::transmute::<PlannedStmt<'_>, PlannedStmt<'static>>(copied)
        });
    }
    Ok(out)
}

/// `portal_define_query_select` seam body — the cursor (`DECLARE CURSOR`) case:
/// copy the single working-context `plan` and `source_text` into the portal's
/// own context and define the query (always SELECT, no prepStmtName, no
/// CachedPlan). C: `plan = copyObject(plan); queryString = pstrdup(sourceText);
/// PortalDefineQuery(portal, NULL, queryString, CMDTAG_SELECT, list_make1(plan),
/// NULL);`.
fn portal_define_query_select(
    portal: &Portal,
    source_text: &str,
    plan: PlannedStmt<'_>,
) -> PgResult<()> {
    let stmts = copy_stmts_into_portal_context(portal, core::slice::from_ref(&plan))?;
    PortalDefineQuery(
        portal,
        None,
        source_text.to_owned(),
        CMDTAG_SELECT,
        Some(stmts),
        CachedPlanHandle::NULL,
    );
    Ok(())
}

/// `portal_define_query_list` seam body — the general `PortalDefineQuery` bridge
/// for an arbitrary planned-statement list and command tag
/// (`exec_simple_query`/`exec_bind_message`). Copies `stmts` and `source_text`
/// into the portal's own context when the plans are not owned by a cached plan
/// (`cplan` NULL); when `cplan` is non-NULL the plans are owned by the cached
/// plan and the list is interned into the portal context the same way (C does
/// the `copyObject` into the portal context regardless; the cached plan keeps
/// its own copy alive via the refcount, recorded by `cplan`).
fn portal_define_query_list(
    portal: &Portal,
    prep_stmt_name: Option<&str>,
    source_text: &str,
    command_tag: CommandTag,
    stmts: &[PlannedStmt<'_>],
    cplan: CachedPlanHandle,
) -> PgResult<()> {
    let copied = copy_stmts_into_portal_context(portal, stmts)?;
    PortalDefineQuery(
        portal,
        prep_stmt_name.map(str::to_owned),
        source_text.to_owned(),
        command_tag,
        Some(copied),
        cplan,
    );
    Ok(())
}

/// `params = copyParamList(params)` after switching to the portal context
/// (`copy_param_list_into_portal` seam body). The portal-facing
/// `ParamListInfo` payload (`types_nodes::portalcmds::ParamListInfoData`) is an
/// `Rc`-shared, not-yet-modeled opaque value; copying it into the portal
/// context is modeled by cloning the refcounted handle (the payload is owned
/// behind the `Rc`, not arena-bound). `None` in → `None` out (the C NULL).
fn copy_param_list_into_portal(
    _portal: &Portal,
    params: types_nodes::portalcmds::ParamListInfo,
) -> PgResult<types_nodes::portalcmds::ParamListInfo> {
    Ok(params)
}

/// `oldcxt = MemoryContextSwitchTo(portal->holdContext);
/// portal->tupDesc = CreateTupleDescCopy(portal->tupDesc);
/// MemoryContextSwitchTo(oldcxt);` (`copy_tup_desc_into_hold_context` seam
/// body) — re-copy the portal's existing result descriptor (currently in
/// executor/portal memory) into its `holdContext` so it survives the executor
/// shutdown, storing the copy back on the portal.
fn copy_tup_desc_into_hold_context(portal: &Portal) -> PgResult<()> {
    let copy: Option<types_tuple::heaptuple::TupleDescData<'static>> = {
        let p = portal.borrow();
        match &p.tupDesc {
            None => None,
            Some(td) => {
                let ctx = p.holdContext.as_ref().ok_or_else(|| {
                    ereport(ERROR)
                        .errmsg_internal(
                            "copy_tup_desc_into_hold_context: portal has no holdContext",
                        )
                        .into_error()
                })?;
                let copied = td.clone_in(ctx.mcx())?;
                // SAFETY: `copied` lives in `holdContext` (real owned heap,
                // freed by its own `Drop`); the `tupDesc` field is dropped
                // before `holdContext` (PortalData::drop ordering), so it never
                // outlives that arena. `'static` is the hold-context-lived
                // marker, like `holdStore`.
                Some(unsafe {
                    core::mem::transmute::<
                        types_tuple::heaptuple::TupleDescData<'_>,
                        types_tuple::heaptuple::TupleDescData<'static>,
                    >(copied)
                })
            }
        }
    };
    portal.borrow_mut().tupDesc = copy;
    Ok(())
}

/// `PortalReleaseCachedPlan` — release a portal's cached-plan reference, if any
/// (portalmem.c:310).
fn PortalReleaseCachedPlan(portal: &PortalCellMut<'_>) {
    let mut p = portal.0.borrow_mut();
    if !p.cplan.is_null() {
        plancache_seam::release_cached_plan::call(p.cplan);
        p.cplan = CachedPlanHandle::NULL;
        // also clear portal->stmts which is now a dangling reference
        p.stmts = None;
    }
}

/// Thin wrapper carrying a `&Portal` for the `PortalReleaseCachedPlan` helper so
/// it can be called on a borrowed handle without re-borrowing conflicts.
struct PortalCellMut<'a>(&'a Portal);

fn release_cached_plan(portal: &Portal) {
    PortalReleaseCachedPlan(&PortalCellMut(portal));
}

/// `PortalCreateHoldStore` — create the tuplestore for a portal
/// (portalmem.c:331).
pub fn PortalCreateHoldStore(portal: &Portal) -> PgResult<()> {
    let exists = TOP_PORTAL_CONTEXT.with(|c| c.borrow().is_some());
    if !exists {
        return Err(ereport(ERROR)
            .errmsg_internal("PortalCreateHoldStore: EnablePortalManager has not run")
            .into_error());
    }

    let random_access = {
        let mut p = portal.borrow_mut();
        debug_assert!(p.holdContext.is_none());
        debug_assert!(p.holdStore.is_none());
        debug_assert!(p.holdSnapshot.is_none());

        // Create the memory context used for storage of the tuple set. Note
        // this is NOT a child of the portal's portalContext: it is created
        // under TopPortalContext so it can outlive the source transaction.
        let hold_context = TOP_PORTAL_CONTEXT.with(|c| {
            c.borrow()
                .as_ref()
                .map(|top| top.new_child("PortalHoldContext"))
        });
        p.holdContext = hold_context;
        (p.cursorOptions & CURSOR_OPT_SCROLL) != 0
    };

    // MemoryContextSwitchTo(holdContext): the C switches the current context so
    // tuplestore_begin_heap pallocs the store there. Here the store is created
    // by the tuplestore owner (seam); allocation targeting is the owner's
    // concern once it lands, so the switch is a no-op for portalmem.
    let store = tuplestore_seam::tuplestore_begin_heap::call(random_access);
    portal.borrow_mut().holdStore = Some(store);
    Ok(())
}

// ===========================================================================
// Live-field accessors (driver-facing). Each mirrors one C field read/write
// on the live Portal handle.
// ===========================================================================

/// `portal->tupDesc = CreateTupleDescCopy(tupdesc)` — store a deep copy of the
/// result tuple descriptor in the portal's `portalContext`. `PortalStart` sets
/// it from `ExecutorStart`'s computed descriptor / `ExecCleanTypeFromTL` /
/// `UtilityTupleDescriptor`. `None` clears it (the `PORTAL_MULTI_QUERY` leg).
///
/// The portal field is `'static` because the copy lives in `portalContext`,
/// which is owned by this `PortalData` and dropped with it (in `PortalDrop`);
/// the copy never outlives the context, so extending the clone's lifetime to
/// the field's `'static` is sound (the C `TupleDesc` in `portalContext` has the
/// same lifetime relationship).
pub fn portal_set_tup_desc(
    portal: &Portal,
    tupdesc: Option<&types_tuple::heaptuple::TupleDescData<'_>>,
) -> PgResult<()> {
    let copy: Option<types_tuple::heaptuple::TupleDescData<'static>> = match tupdesc {
        None => None,
        Some(td) => {
            let p = portal.borrow();
            let ctx = p.portalContext.as_ref().ok_or_else(|| {
                ereport(ERROR)
                    .errmsg_internal("portal_set_tup_desc: portal has no portalContext")
                    .into_error()
            })?;
            let copied = td.clone_in(ctx.mcx())?;
            // SAFETY: `copied` is allocated in `portalContext`, which this
            // `PortalData` owns and drops together with the `tupDesc` field
            // (PortalDrop drops both); the descriptor therefore never outlives
            // its arena. The `'static` on the field is portalmem's marker for
            // "portal-context-lived", mirroring `stmts`/`holdStore`.
            Some(unsafe {
                core::mem::transmute::<
                    types_tuple::heaptuple::TupleDescData<'_>,
                    types_tuple::heaptuple::TupleDescData<'static>,
                >(copied)
            })
        }
    };
    portal.borrow_mut().tupDesc = copy;
    Ok(())
}

/// Build the portal's result tuple descriptor in its own `portalContext` and
/// store it, under a single portal borrow. `build` is the caller's
/// `ExecCleanTypeFromTL(pstmt->planTree->targetlist)` /
/// `UtilityTupleDescriptor(pstmt->utilityStmt)` leg: given the portalContext's
/// [`Mcx`](mcx::Mcx) and the portal's primary statement list, it produces the
/// descriptor allocated in that context.
///
/// This exists because the descriptor a `PortalStart` leg builds is bound to
/// `portalContext`'s lifetime — it cannot escape the portal borrow to be
/// handed to [`portal_set_tup_desc`]. portalmem owns the `portalContext` arena
/// and the `'static`-marked `tupDesc` field, so the unsafe arena-lifetime
/// marshaling lives here (mirroring [`portal_set_tup_desc`]), never in the
/// driver.
pub fn set_result_tup_desc_with(
    portal: &Portal,
    build: &mut dyn for<'m> FnMut(
        mcx::Mcx<'m>,
        &[types_nodes::nodeindexscan::PlannedStmt<'m>],
    ) -> PgResult<types_tuple::heaptuple::TupleDesc<'m>>,
) -> PgResult<()> {
    let mut p = portal.borrow_mut();
    let ctx = p.portalContext.as_ref().ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal("set_result_tup_desc_with: portal has no portalContext")
            .into_error()
    })?;
    // SAFETY of the lifetime juggling below: `stmts` is `Vec<PlannedStmt<'static>>`
    // (the portal-context-lived marker) and `ctx.mcx()` borrows `ctx`; both live
    // for the duration of this borrow. The descriptor `build` returns is
    // allocated in `ctx` (the portalContext), which this `PortalData` owns and
    // drops with the `tupDesc` field — so extending the result to the field's
    // `'static` marker is sound, exactly as `portal_set_tup_desc` argues.
    let ctx_ptr: *const mcx::MemoryContext = ctx;
    let mcx = unsafe { (*ctx_ptr).mcx() };
    let stmts: &[types_nodes::nodeindexscan::PlannedStmt<'static>] =
        p.stmts.as_deref().unwrap_or(&[]);
    let built = build(mcx, stmts)?;
    let copy: Option<types_tuple::heaptuple::TupleDescData<'static>> = match built {
        None => None,
        Some(b) => {
            // The built descriptor already lives in `ctx` (the portalContext);
            // move its value out by cloning the deref into the same context,
            // then extend to the field's `'static` marker (sound — same arena).
            let owned = (*b).clone_in(mcx)?;
            Some(unsafe {
                core::mem::transmute::<
                    types_tuple::heaptuple::TupleDescData<'_>,
                    types_tuple::heaptuple::TupleDescData<'static>,
                >(owned)
            })
        }
    };
    p.tupDesc = copy;
    Ok(())
}

pub fn portal_get_strategy(portal: &Portal) -> PortalStrategy {
    portal.borrow().strategy
}
pub fn portal_set_strategy(portal: &Portal, strategy: PortalStrategy) {
    portal.borrow_mut().strategy = strategy;
}
pub fn portal_get_at_start(portal: &Portal) -> bool {
    portal.borrow().atStart
}
pub fn portal_set_at_start(portal: &Portal, atStart: bool) {
    portal.borrow_mut().atStart = atStart;
}
pub fn portal_get_at_end(portal: &Portal) -> bool {
    portal.borrow().atEnd
}
pub fn portal_set_at_end(portal: &Portal, atEnd: bool) {
    portal.borrow_mut().atEnd = atEnd;
}
pub fn portal_get_portal_pos(portal: &Portal) -> u64 {
    portal.borrow().portalPos
}
pub fn portal_set_portal_pos(portal: &Portal, portalPos: u64) {
    portal.borrow_mut().portalPos = portalPos;
}
/// `portal->stmts != NIL` operand: the number of planned statements held by the
/// portal (`0` == C `NIL`). The primary-stmt walk itself is
/// [`PortalGetPrimaryStmt`].
pub fn portal_num_stmts(portal: &Portal) -> usize {
    portal.borrow().stmts.as_ref().map_or(0, |v| v.len())
}
pub fn portal_get_qc(portal: &Portal) -> QueryCompletion {
    portal.borrow().qc
}
pub fn portal_set_qc(portal: &Portal, qc: QueryCompletion) {
    portal.borrow_mut().qc = qc;
}
pub fn portal_get_command_tag(portal: &Portal) -> CommandTag {
    portal.borrow().commandTag
}
pub fn portal_set_command_tag(portal: &Portal, commandTag: CommandTag) {
    portal.borrow_mut().commandTag = commandTag;
}
pub fn portal_get_cursor_options(portal: &Portal) -> i32 {
    portal.borrow().cursorOptions
}
pub fn portal_set_cursor_options(portal: &Portal, cursorOptions: i32) {
    portal.borrow_mut().cursorOptions = cursorOptions;
}
pub fn portal_get_create_subid(portal: &Portal) -> SubTransactionId {
    portal.borrow().createSubid
}
pub fn portal_get_status(portal: &Portal) -> PortalStatus {
    portal.borrow().status
}
/// `portal->status = status` — raw write for the few driver paths that set
/// status directly (e.g. `PortalRun` resetting `PORTAL_READY`).
pub fn portal_set_status(portal: &Portal, status: PortalStatus) {
    portal.borrow_mut().status = status;
}
pub fn portal_get_resowner(portal: &Portal) -> ResourceOwner {
    portal.borrow().resowner.clone()
}
pub fn portal_set_resowner(portal: &Portal, resowner: ResourceOwner) {
    portal.borrow_mut().resowner = resowner;
}
pub fn portal_get_create_level(portal: &Portal) -> i32 {
    portal.borrow().createLevel
}
pub fn portal_get_visible(portal: &Portal) -> bool {
    portal.borrow().visible
}
/// `PortalSetVisible` analog — used by `exec_simple_query` to hide the unnamed
/// portal it runs each statement in.
pub fn PortalSetVisible(portal: &Portal, visible: bool) {
    portal.borrow_mut().visible = visible;
}
pub fn portal_get_source_text(portal: &Portal) -> Option<String> {
    portal.borrow().sourceText.clone()
}

/// `PinPortal` — protect a portal from dropping (portalmem.c:371).
pub fn PinPortal(portal: &Portal) -> PgResult<()> {
    if portal.borrow().portalPinned {
        return Err(ereport(ERROR)
            .errmsg_internal("portal already pinned")
            .into_error());
    }
    portal.borrow_mut().portalPinned = true;
    Ok(())
}

/// `UnpinPortal` (portalmem.c:380).
pub fn UnpinPortal(portal: &Portal) -> PgResult<()> {
    if !portal.borrow().portalPinned {
        return Err(ereport(ERROR)
            .errmsg_internal("portal not pinned")
            .into_error());
    }
    portal.borrow_mut().portalPinned = false;
    Ok(())
}

/// `MarkPortalActive` — READY → ACTIVE (portalmem.c:395).
pub fn MarkPortalActive(portal: &Portal) -> PgResult<()> {
    // For safety, this is a runtime test not just an Assert.
    if portal.borrow().status != PORTAL_READY {
        let name = portal.borrow().name.clone();
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("portal \"{name}\" cannot be run"))
            .into_error());
    }
    let subid = xact_seam::get_current_sub_transaction_id::call();
    let mut p = portal.borrow_mut();
    p.status = PORTAL_ACTIVE;
    p.activeSubid = subid;
    Ok(())
}

/// `MarkPortalDone` — ACTIVE → DONE (portalmem.c:414).
pub fn MarkPortalDone(portal: &Portal) -> PgResult<()> {
    {
        let mut p = portal.borrow_mut();
        debug_assert_eq!(p.status, PORTAL_ACTIVE);
        p.status = PORTAL_DONE;
    }

    // Allow portalcmds.c to clean up the state it knows about.
    run_portal_cleanup_hook(portal)?;
    Ok(())
}

/// `MarkPortalFailed` — → FAILED (portalmem.c:442).
pub fn MarkPortalFailed(portal: &Portal) -> PgResult<()> {
    {
        let mut p = portal.borrow_mut();
        debug_assert!(p.status != PORTAL_DONE);
        p.status = PORTAL_FAILED;
    }

    run_portal_cleanup_hook(portal)?;
    Ok(())
}

/// `PortalDrop` — destroy the portal (portalmem.c:468).
pub fn PortalDrop(portal: &Portal, isTopCommit: bool) -> PgResult<()> {
    // Don't allow dropping a pinned portal.
    if portal.borrow().portalPinned {
        let name = portal.borrow().name.clone();
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_CURSOR_STATE)
            .errmsg(format!("cannot drop pinned portal \"{name}\""))
            .into_error());
    }

    // Not sure if the PORTAL_ACTIVE case can validly happen or not...
    if portal.borrow().status == PORTAL_ACTIVE {
        let name = portal.borrow().name.clone();
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_CURSOR_STATE)
            .errmsg(format!("cannot drop active portal \"{name}\""))
            .into_error());
    }

    // Allow portalcmds.c to clean up the state it knows about.
    run_portal_cleanup_hook(portal)?;

    // There shouldn't be an active snapshot anymore, except after error.
    debug_assert!(portal.borrow().portalSnapshot.is_none() || !isTopCommit);

    // Remove portal from hash table.
    portal_hash_table_delete(portal)?;

    // drop cached plan reference, if any
    release_cached_plan(portal);

    // If portal has a snapshot protecting its data, release that.
    let hold_snapshot = portal.borrow().holdSnapshot.clone();
    if let Some(snap) = hold_snapshot {
        let resowner = portal.borrow().resowner.clone();
        if !resowner.is_null() {
            snapmgr_seam::unregister_snapshot_from_owner::call(snap, resowner);
        }
        portal.borrow_mut().holdSnapshot = None;
    }

    // Release any resources still attached to the portal.
    let (resowner, status) = {
        let p = portal.borrow();
        (p.resowner.clone(), p.status)
    };
    if !resowner.is_null() && (!isTopCommit || status == PORTAL_FAILED) {
        let is_commit = status != PORTAL_FAILED;

        resowner_seam::resource_owner_release::call(
            resowner.clone(),
            RESOURCE_RELEASE_BEFORE_LOCKS,
            is_commit,
            false,
        );
        resowner_seam::resource_owner_release::call(
            resowner.clone(),
            RESOURCE_RELEASE_LOCKS,
            is_commit,
            false,
        );
        resowner_seam::resource_owner_release::call(
            resowner.clone(),
            RESOURCE_RELEASE_AFTER_LOCKS,
            is_commit,
            false,
        );
        resowner_seam::resource_owner_delete::call(resowner);
    }
    portal.borrow_mut().resowner = ResourceOwner::default();

    // Delete tuplestore if present. We do this even under error conditions;
    // since the tuplestore uses cross-transaction storage, its temp files need
    // to be explicitly deleted.
    let has_store = portal.borrow().holdStore.is_some();
    if has_store {
        if portal.borrow().holdContext.is_none() {
            return Err(ereport(ERROR)
                .errmsg_internal("PortalDrop: holdStore set without holdContext")
                .into_error());
        }
        // MemoryContextSwitchTo(holdContext) then tuplestore_end(holdStore):
        // releasing the owned store (drop) frees it + its temp files (RAII).
        portal.borrow_mut().holdStore = None;
    }

    // The result tuple descriptor lives in either portalContext or
    // holdContext (CreateTupleDescCopy / copy_tup_desc_into_hold_context); drop
    // it before either context so the copy is freed through a still-live arena.
    portal.borrow_mut().tupDesc = None;

    // delete tuplestore storage, if any (drop the owned holdContext arena)
    portal.borrow_mut().holdContext = None;

    // release subsidiary storage (drop the owned portalContext arena)
    if portal.borrow().portalContext.is_none() {
        return Err(ereport(ERROR)
            .errmsg_internal(
                "PortalDrop: portal has no portalContext (CreatePortal always assigns one)",
            )
            .into_error());
    }
    // The interned plan list and copied parameter list live in portalContext;
    // drop them before the context so they are freed through a live arena (C:
    // MemoryContextDelete(portalContext) frees them and nothing reads them
    // again).
    {
        let mut p = portal.borrow_mut();
        p.stmts = None;
        p.portalParams = None;
    }
    portal.borrow_mut().portalContext = None;

    // release portal struct (it's in TopPortalContext): the table no longer
    // holds it, so dropping the last handle frees the record.
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
        let portals = portal_handles()?;
        let mut dropped_one = false;
        for portal in portals {
            // Can't close the active portal (the one running the command).
            if portal.borrow().status == PORTAL_ACTIVE {
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
fn HoldPortal(portal: &Portal) -> PgResult<()> {
    // PersistHoldablePortal() must release all resources local to the creating
    // transaction.
    PortalCreateHoldStore(portal)?;
    portalcmds_seam::persist_holdable_portal::call(portal.clone())?;

    // drop cached plan reference, if any
    release_cached_plan(portal);

    let mut p = portal.borrow_mut();
    // The portal will no longer have its own resources.
    p.resowner = ResourceOwner::default();
    // Mark it as not belonging to this transaction.
    p.createSubid = InvalidSubTransactionId;
    p.activeSubid = InvalidSubTransactionId;
    p.createLevel = 0;
    Ok(())
}

/// `PreCommit_Portals` — pre-commit processing (portalmem.c:677). Returns true
/// if any portals changed state (possibly running user code).
pub fn PreCommit_Portals(isPrepare: bool) -> PgResult<bool> {
    let mut result = false;

    'restart: loop {
        let portals = portal_handles()?;
        for portal in portals {
            let (pinned, auto_held, status, cursor_options, create_subid) = {
                let p = portal.borrow();
                (
                    p.portalPinned,
                    p.autoHeld,
                    p.status,
                    p.cursorOptions,
                    p.createSubid,
                )
            };

            // There should be no pinned portals anymore. Auto-held allowed.
            if pinned && !auto_held {
                return Err(ereport(ERROR)
                    .errmsg_internal("cannot commit while a portal is pinned")
                    .into_error());
            }

            // Do not touch active portals.
            if status == PORTAL_ACTIVE {
                let (hold_snapshot, resowner) = {
                    let p = portal.borrow();
                    (p.holdSnapshot.clone(), p.resowner.clone())
                };
                if let Some(snap) = hold_snapshot {
                    if !resowner.is_null() {
                        snapmgr_seam::unregister_snapshot_from_owner::call(snap, resowner);
                    }
                    portal.borrow_mut().holdSnapshot = None;
                }
                let mut p = portal.borrow_mut();
                p.resowner = ResourceOwner::default();
                // Clear portalSnapshot too, for cleanliness.
                p.portalSnapshot = None;
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
    let portals = portal_handles()?;
    for portal in portals {
        let (mut status, create_subid, auto_held) = {
            let p = portal.borrow();
            (p.status, p.createSubid, p.autoHeld)
        };

        // When elog(FATAL) is in progress, set the active portal to failed.
        if status == PORTAL_ACTIVE && backend_storage_ipc_dsm_core::ipc::shmem_exit_inprogress() {
            MarkPortalFailed(&portal)?;
            status = portal.borrow().status;
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
        run_portal_cleanup_hook(&portal)?;

        // drop cached plan reference, if any
        release_cached_plan(&portal);

        // Resources will be released in the upcoming transaction-wide cleanup.
        let status = {
            let mut p = portal.borrow_mut();
            p.resowner = ResourceOwner::default();
            p.status
        };

        // Release any memory in subsidiary contexts, but leave active alone.
        if status != PORTAL_ACTIVE {
            let mut p = portal.borrow_mut();
            if let Some(ctx) = &mut p.portalContext {
                ctx.reset();
            }
        }
    }
    Ok(())
}

/// `AtCleanup_Portals` (portalmem.c:858). Single scan, no restart (no user code
/// runs here).
pub fn AtCleanup_Portals() -> PgResult<()> {
    let portals = portal_handles()?;
    for portal in portals {
        let (status, create_subid, auto_held, resowner_null) = {
            let p = portal.borrow();
            (p.status, p.createSubid, p.autoHeld, p.resowner.is_null())
        };

        // Do not touch active portals.
        if status == PORTAL_ACTIVE {
            continue;
        }

        // Do nothing to cursors held over from a previous transaction or
        // auto-held ones.
        if create_subid == InvalidSubTransactionId || auto_held {
            debug_assert!(status != PORTAL_ACTIVE);
            debug_assert!(resowner_null);
            continue;
        }

        // If a portal is still pinned, forcibly unpin it.
        {
            let mut p = portal.borrow_mut();
            if p.portalPinned {
                p.portalPinned = false;
            }
        }

        // We had better not call any user-defined code during cleanup; if the
        // cleanup hook hasn't been run yet, skip it.
        if portal.borrow().cleanup.is_some() {
            let name = portal.borrow().name.clone();
            elog(WARNING, format!("skipping cleanup for portal \"{name}\""))?;
            portal.borrow_mut().cleanup = PortalCleanupHook::NONE;
        }

        // Zap it.
        PortalDrop(&portal, false)?;
    }
    Ok(())
}

/// `PortalErrorCleanup` — portal cleanup when returning to the main loop on
/// error (portalmem.c:917). Single scan, no restart.
pub fn PortalErrorCleanup() -> PgResult<()> {
    let portals = portal_handles()?;
    for portal in portals {
        if portal.borrow().autoHeld {
            portal.borrow_mut().portalPinned = false;
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
    parentXactOwner: ResourceOwner,
) -> PgResult<()> {
    let portals = portal_handles()?;
    for portal in portals {
        let owner = {
            let mut p = portal.borrow_mut();
            let was_mine = p.createSubid == mySubid;
            if was_mine {
                p.createSubid = parentSubid;
                p.createLevel = parentLevel;
            }
            if p.activeSubid == mySubid {
                p.activeSubid = parentSubid;
            }
            if was_mine && !p.resowner.is_null() {
                Some(p.resowner.clone())
            } else {
                None
            }
        };
        if let Some(owner) = owner {
            resowner_seam::resource_owner_new_parent::call(owner, parentXactOwner.clone());
        }
    }
    Ok(())
}

/// `AtSubAbort_Portals` (portalmem.c:979).
pub fn AtSubAbort_Portals(
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
    myXactOwner: ResourceOwner,
    parentXactOwner: ResourceOwner,
) -> PgResult<()> {
    let _ = parentXactOwner;

    let portals = portal_handles()?;
    for portal in portals {
        let create_subid = portal.borrow().createSubid;

        // Was it created in this subtransaction?
        if create_subid != mySubid {
            // No, but maybe it was used in this subtransaction?
            let active_subid = portal.borrow().activeSubid;
            if active_subid == mySubid {
                // Maintain activeSubid until the portal is removed.
                portal.borrow_mut().activeSubid = parentSubid;

                // Force a left-ACTIVE upper-level portal into FAILED state.
                if portal.borrow().status == PORTAL_ACTIVE {
                    MarkPortalFailed(&portal)?;
                }

                // If we failed it during the current subtransaction, reattach
                // its resource owner to the current subxact's owner.
                let owner = {
                    let mut p = portal.borrow_mut();
                    if p.status == PORTAL_FAILED && !p.resowner.is_null() {
                        let o = p.resowner.clone();
                        p.resowner = ResourceOwner::default();
                        Some(o)
                    } else {
                        None
                    }
                };
                if let Some(owner) = owner {
                    resowner_seam::resource_owner_new_parent::call(owner, myXactOwner.clone());
                }
            }
            // Done if it wasn't created in this subtransaction.
            continue;
        }

        // Force any live portals of my own subtransaction into FAILED state.
        let status = portal.borrow().status;
        if status == PORTAL_READY || status == PORTAL_ACTIVE {
            MarkPortalFailed(&portal)?;
        }

        // Allow portalcmds.c to clean up, if we haven't already.
        run_portal_cleanup_hook(&portal)?;

        // drop cached plan reference, if any
        release_cached_plan(&portal);

        // Resources will be released in the upcoming transaction-wide cleanup.
        let mut p = portal.borrow_mut();
        p.resowner = ResourceOwner::default();
        // Release any memory in subsidiary contexts, such as executor state.
        if let Some(ctx) = &mut p.portalContext {
            ctx.reset();
        }
    }
    Ok(())
}

/// `AtSubCleanup_Portals` (portalmem.c:1092). Single scan, no restart.
pub fn AtSubCleanup_Portals(mySubid: SubTransactionId) -> PgResult<()> {
    let portals = portal_handles()?;
    for portal in portals {
        if portal.borrow().createSubid != mySubid {
            continue;
        }

        // If a portal is still pinned, forcibly unpin it.
        {
            let mut p = portal.borrow_mut();
            if p.portalPinned {
                p.portalPinned = false;
            }
        }

        // We had better not call any user-defined code during cleanup.
        if portal.borrow().cleanup.is_some() {
            let name = portal.borrow().name.clone();
            elog(WARNING, format!("skipping cleanup for portal \"{name}\""))?;
            portal.borrow_mut().cleanup = PortalCleanupHook::NONE;
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
pub fn pg_cursor(fcinfo: FcinfoHandle) -> PgResult<Datum<'static>> {
    let portals = portal_handles()?;
    let workspace = MemoryContext::new("pg_cursor");
    let mcx = workspace.mcx();

    let mut rows: PgVec<PgCursorRow> = mcx::vec_with_capacity_in(mcx, portals.len())?;
    for portal in &portals {
        let p = portal.borrow();
        // report only "visible" entries
        if !p.visible {
            continue;
        }
        // ignore it if PortalDefineQuery hasn't been called yet
        let statement = match &p.sourceText {
            Some(s) => s.clone(),
            None => continue,
        };
        rows.push(PgCursorRow {
            name: p.name.clone(),
            statement,
            is_holdable: (p.cursorOptions & CURSOR_OPT_HOLD) != 0,
            is_binary: (p.cursorOptions & CURSOR_OPT_BINARY) != 0,
            is_scrollable: (p.cursorOptions & CURSOR_OPT_SCROLL) != 0,
            creation_time: p.creation_time,
        });
    }

    pg_cursor_srf(fcinfo, &rows)
}

/// The `pg_cursor()` SRF body: `InitMaterializedSRF` + per-row `Datum`
/// conversions + `tuplestore_putvalues` (the fmgr/`Datum` value layer, a
/// project-wide deferral). Given the already-collected visible rows, returns the
/// SRF result Datum. Stubbed until the fmgr/`Datum` value layer lands.
fn pg_cursor_srf(_fcinfo: FcinfoHandle, _rows: &[PgCursorRow]) -> PgResult<Datum<'static>> {
    Ok(Datum::null())
}

/// `ThereAreNoReadyPortals` (portalmem.c:1171).
pub fn ThereAreNoReadyPortals() -> PgResult<bool> {
    let portals = portal_handles()?;
    for portal in portals {
        if portal.borrow().status == PORTAL_READY {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `HoldPinnedPortals` — hold all pinned portals (portalmem.c:1207).
pub fn HoldPinnedPortals() -> PgResult<()> {
    let portals = portal_handles()?;
    for portal in portals {
        let (pinned, auto_held, strategy, status) = {
            let p = portal.borrow();
            (p.portalPinned, p.autoHeld, p.strategy, p.status)
        };
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
            portal.borrow_mut().autoHeld = true;
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
    let portals = portal_handles()?;
    for portal in portals {
        let mut p = portal.borrow_mut();
        if p.portalSnapshot.is_some() {
            p.portalSnapshot = None;
            numPortalSnaps += 1;
        }
        // portal->holdSnapshot will be cleaned up in PreCommit_Portals.
    }

    // Now, pop all the active snapshots.
    while snapmgr_seam::active_snapshot_set::call() {
        snapmgr_seam::pop_active_snapshot::call()?;
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

// ===========================================================================
// Seam implementations called by the cyclic portalcmds (cursor command) unit.
// The portal crosses as the shared `types_portal::Portal` open handle.
// ===========================================================================

/// `MemoryContextDeleteChildren(portal->portalContext)` — release subsidiary
/// memory of the portal's context (the portalmem-owned arena), modeled as
/// `MemoryContext::reset()`.
fn memory_context_delete_children(portal: &Portal) -> PgResult<()> {
    let mut p = portal.borrow_mut();
    if let Some(ctx) = &mut p.portalContext {
        ctx.reset();
    }
    Ok(())
}

/// Run `f` with `portal` installed as `ActivePortal` and its `portalContext` as
/// `PortalContext`, restoring the previous values afterwards and on error (C's
/// save/set/restore around the `PersistHoldablePortal` PG_TRY block).
fn with_portal_globals(
    portal: &Portal,
    f: &mut dyn FnMut() -> PgResult<()>,
) -> PgResult<()> {
    let saved_active = ACTIVE_PORTAL.with(|c| c.borrow_mut().replace(portal.clone()));
    let saved_ctx = PORTAL_CONTEXT_OWNER.with(|c| c.borrow_mut().replace(portal.clone()));

    let result = f();

    ACTIVE_PORTAL.with(|c| *c.borrow_mut() = saved_active);
    PORTAL_CONTEXT_OWNER.with(|c| *c.borrow_mut() = saved_ctx);
    result
}

/// Read the per-backend `ActivePortal` global (C `Portal ActivePortal`): run
/// `f` with the currently-active portal (`None` is the C `NULL`), returning its
/// result. A scoped reader (not an ambient `&'static` getter): the portal is
/// cloned out of the thread-local so the closure never holds its borrow.
/// `EnsurePortalSnapshotExists` reads `ActivePortal` through this.
pub fn with_active_portal<R>(f: impl FnOnce(Option<&Portal>) -> R) -> R {
    let active = ACTIVE_PORTAL.with(|c| c.borrow().clone());
    f(active.as_ref())
}

/// `ErrorLocation` for an `ereport(...).finish(...)` site
/// (`__FILE__`/`__LINE__`/`__func__`).
fn errloc(lineno: i32, funcname: &'static str) -> types_error::ErrorLocation {
    types_error::ErrorLocation::new("portalmem.c", lineno, funcname)
}

pub use seams_install::init_seams;

#[cfg(test)]
mod tests;
