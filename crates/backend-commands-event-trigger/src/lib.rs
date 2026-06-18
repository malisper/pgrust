#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `backend/commands/event_trigger.c` — the command-collection / firing
//! *fences* called by `ProcessUtilitySlow` and the per-command collection sites.
//!
//! This unit lands the bounded standalone spine: the six fences
//! `EventTriggerBeginCompleteQuery` / `EventTriggerEndCompleteQuery` /
//! `EventTriggerDDLCommandStart` / `EventTriggerDDLCommandEnd` /
//! `EventTriggerSQLDrop` / `EventTriggerCollectSimpleCommand`, plus the
//! backend-local `currentEventTriggerState` stack they read — the
//! [`EventTriggerQueryState`] carrier (its `SQLDropList` and `commandList`), held
//! in a thread-local exactly as C keeps the file-static `currentEventTriggerState`.
//!
//! In standalone single-user mode (the only mode this repo boots) the firing
//! fences begin with `if (!IsUnderPostmaster || !event_triggers) return;`, so
//! they are real no-ops there; `EventTriggerBeginCompleteQuery` calls
//! `EventCacheLookup` (real) and — with no event triggers in a fresh cluster —
//! returns `false`, leaving `currentEventTriggerState == NULL`, which makes
//! `EventTriggerCollectSimpleCommand`'s `collecting()` gate a no-op too. Every
//! fence is a REAL function reading the live state, never a stub.
//!
//! The post-gate firing tail (`EventTriggerCommonSetup` + `EventTriggerInvoke`)
//! and the deeper collection/SRF surface (CREATE-collection, ALTER TABLE
//! sub-command threading, `pg_event_trigger_dropped_objects`,
//! `pg_event_trigger_ddl_commands`, the CREATE/ALTER/owner DDL) are a distinct
//! sub-campaign (fmgr dispatch, snapshot, `CreateCommandTag`,
//! `session_replication_role`, bitmapset, the owned `CollectedCommand` deparse
//! union). The firing tail crosses [`fire_seams::event_trigger_fire`] with a
//! loud-panic default — unreachable while `IsUnderPostmaster` is false.

use core::cell::RefCell;

use mcx::MemoryContext;
use types_catalog::catalog::{
    AUTH_ID_RELATION_ID, AUTH_MEM_RELATION_ID, DATABASE_RELATION_ID, EVENT_TRIGGER_RELATION_ID,
    PARAMETER_ACL_RELATION_ID, TABLE_SPACE_RELATION_ID,
};
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::{InvalidOid, Oid};
use types_error::PgResult;
use types_evtcache::EventTriggerEvent;
use types_nodes::nodes::Node;
use types_nodes::parsenodes::ObjectType;

use backend_commands_event_trigger_fire_seams as fire_seams;
use backend_commands_extension_seams as extension_seams;
use backend_utils_cache_evtcache_seams as evtcache_seams;
use backend_utils_init_small_seams as init_small_seams;
use backend_utils_misc_guc_tables::vars;

// ===========================================================================
// Backend-local query state (file-static in C).
// ===========================================================================

/// One object dropped by the current command (`typedef struct SQLDropObject`,
/// event_trigger.c) — owned value form. Populated by
/// `EventTriggerSQLDropAddObject` (the deeper collection sub-campaign); the
/// `sql_drop` fence only inspects whether the list is non-empty. The detail
/// fields are written by that sub-campaign and read by
/// `pg_event_trigger_dropped_objects`, neither of which is in this spine.
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct SQLDropObject {
    address: ObjectAddress,
    schemaname: Option<String>,
    objname: Option<String>,
    objidentity: Option<String>,
    objecttype: Option<String>,
    addrnames: Option<Vec<String>>,
    addrargs: Option<Vec<String>>,
    original: bool,
    normal: bool,
    istemp: bool,
}

/// One collected command (`typedef struct CollectedCommand`,
/// tcop/deparse_utility.h) — the simple-command form that
/// [`event_trigger_collect_simple_command`] appends. The parse tree is deep
/// copied into the owning state's private `cmd_cxt` arena (the C
/// `currentEventTriggerState->cxt`), so it survives until
/// `EventTriggerEndCompleteQuery` tears the state down.
#[allow(dead_code)]
struct CollectedCommand {
    in_extension: bool,
    /// `parsetree` — `copyObject`'d into the state's private context, with a
    /// `'static` marker that is the arena-lived lifetime (the arena outlives
    /// every command in `command_list`; see [`EventTriggerQueryState`] drop
    /// ordering).
    parsetree: Node<'static>,
    address: ObjectAddress,
    secondary_object: ObjectAddress,
}

/// `typedef struct EventTriggerQueryState` (event_trigger.c) — backend-local
/// per-query state, kept in [`CURRENT_STATE`] exactly as C keeps the file-static
/// `currentEventTriggerState`.
///
/// **Drop ordering is load-bearing.** `sql_drop_list` and `command_list` hold
/// values (the `CollectedCommand::parsetree` clones) allocated *in* `cmd_cxt`;
/// Rust drops struct fields in declaration order, so `cmd_cxt` is declared last
/// and freed only after every value that lives in it — the analog of C's
/// `MemoryContextDelete(state->cxt)` running after the lists are no longer read.
struct EventTriggerQueryState {
    /// sql_drop list (`SQLDropObject`s, in slist head-pushed order).
    sql_drop_list: Vec<SQLDropObject>,
    in_sql_drop: bool,

    /// table_rewrite: `InvalidOid`, or set for a table_rewrite event. Read by
    /// `EventTriggerTableRewrite` / `pg_event_trigger_table_rewrite_oid`, which
    /// are part of the deeper firing sub-campaign, not this spine.
    #[allow(dead_code)]
    table_rewrite_oid: Oid,
    /// AT_REWRITE reason (read by the table_rewrite sub-campaign).
    #[allow(dead_code)]
    table_rewrite_reason: i32,

    /// Support for command collection.
    command_collection_inhibited: bool,
    /// list of `CollectedCommand`; see deparse_utility.h.
    command_list: Vec<CollectedCommand>,

    /// `state->cxt` — the private arena owning every deep-copied parse tree in
    /// `command_list` / `sql_drop_list`. Declared LAST so it outlives them on
    /// drop (see the struct-level note).
    cmd_cxt: MemoryContext,
}

thread_local! {
    /// `static EventTriggerQueryState *currentEventTriggerState = NULL;`
    ///
    /// The C `previous` linked stack becomes a `Vec` whose last element is the
    /// current state (`currentEventTriggerState`) and whose predecessor is the
    /// `previous` state. Empty means `currentEventTriggerState == NULL`.
    static CURRENT_STATE: RefCell<Vec<EventTriggerQueryState>> = const { RefCell::new(Vec::new()) };
}

/// Whether `currentEventTriggerState != NULL`.
fn state_is_set() -> bool {
    CURRENT_STATE.with(|s| !s.borrow().is_empty())
}

// ===========================================================================
// currentEventTriggerState stack (event_trigger.c) + trackDroppedObjectsNeeded.
// ===========================================================================

/// `trackDroppedObjectsNeeded` (event_trigger.c) — true if any sql_drop /
/// table_rewrite / ddl_command_end event trigger exists.
///
/// C runs `EventCacheLookup` in `CurrentMemoryContext` and discards the result;
/// here the lookup result is charged to a transient context that is dropped
/// when this function returns (only emptiness is inspected).
pub fn track_dropped_objects_needed() -> PgResult<bool> {
    let cxt = MemoryContext::new("event trigger track-dropped lookup");
    let mcx = cxt.mcx();
    let any = !evtcache_seams::event_cache_lookup::call(mcx, EventTriggerEvent::SqlDrop)?.is_empty()
        || !evtcache_seams::event_cache_lookup::call(mcx, EventTriggerEvent::TableRewrite)?
            .is_empty()
        || !evtcache_seams::event_cache_lookup::call(mcx, EventTriggerEvent::DdlCommandEnd)?
            .is_empty();
    Ok(any)
}

/// `EventTriggerBeginCompleteQuery` (event_trigger.c) — install query state if
/// needed; returns whether it was installed.
pub fn event_trigger_begin_complete_query() -> PgResult<bool> {
    // Currently, sql_drop, table_rewrite, ddl_command_end events are the only
    // reason to have event trigger state at all; so if there are none, don't
    // install one. The `EventCacheLookup` rebuild can `ereport`, which the
    // owned model surfaces as `Err` (the C longjmp past this caller).
    if !track_dropped_objects_needed()? {
        return Ok(false);
    }

    let inherited_inhibit = CURRENT_STATE.with(|s| {
        s.borrow()
            .last()
            .map(|st| st.command_collection_inhibited)
            .unwrap_or(false)
    });

    let state = EventTriggerQueryState {
        sql_drop_list: Vec::new(),
        in_sql_drop: false,
        table_rewrite_oid: InvalidOid,
        table_rewrite_reason: 0,
        command_collection_inhibited: inherited_inhibit,
        command_list: Vec::new(),
        cmd_cxt: MemoryContext::new("event trigger state"),
    };

    CURRENT_STATE.with(|s| s.borrow_mut().push(state));

    Ok(true)
}

/// `EventTriggerEndCompleteQuery` (event_trigger.c) — tear down query state.
///
/// The C `MemoryContextDelete(cxt)` that retail-frees the `SQLDropList` /
/// `commandList` is the `Vec`/`MemoryContext` drop here (the popped state's
/// `cmd_cxt` is dropped after its lists, per the struct's drop ordering);
/// popping the stack returns to `previous`.
///
/// Note: it is an error to call this if `EventTriggerBeginCompleteQuery`
/// returned false previously (the caller's `needCleanup` guard enforces that),
/// so the pop is expected to find a state.
pub fn event_trigger_end_complete_query() {
    CURRENT_STATE.with(|s| {
        s.borrow_mut().pop();
    });
}

// ===========================================================================
// Firing fences (event_trigger.c) — gated off in standalone single-user mode.
// ===========================================================================

/// Event triggers are completely disabled in standalone mode and behind the
/// `event_triggers` GUC (see `EventTriggerDDLCommandStart`'s comment in C).
fn event_triggers_active() -> bool {
    init_small_seams::is_under_postmaster::call() && vars::event_triggers.read()
}

/// `EventTriggerDDLCommandStart` (event_trigger.c) — fire ddl_command_start
/// triggers.
pub fn event_trigger_ddl_command_start(parsetree: &Node) -> PgResult<()> {
    if !event_triggers_active() {
        return Ok(());
    }
    fire_seams::event_trigger_fire::call(
        parsetree,
        EventTriggerEvent::DdlCommandStart,
        "ddl_command_start",
    )
}

/// `EventTriggerDDLCommandEnd` (event_trigger.c) — fire ddl_command_end
/// triggers.
pub fn event_trigger_ddl_command_end(parsetree: &Node) -> PgResult<()> {
    if !event_triggers_active() {
        return Ok(());
    }
    // Also do nothing if our state isn't set up; this can happen when an event
    // trigger fires another command from a function, etc.
    if !state_is_set() {
        return Ok(());
    }
    fire_seams::event_trigger_fire::call(
        parsetree,
        EventTriggerEvent::DdlCommandEnd,
        "ddl_command_end",
    )
}

/// `EventTriggerSQLDrop` (event_trigger.c) — fire sql_drop triggers.
pub fn event_trigger_sql_drop(parsetree: &Node) -> PgResult<()> {
    if !event_triggers_active() {
        return Ok(());
    }
    // Use current state to determine whether this event fires at all: do
    // nothing if state isn't set up, or the SQLDropList is empty.
    let have_drops = CURRENT_STATE.with(|s| {
        s.borrow()
            .last()
            .map(|st| !st.sql_drop_list.is_empty())
            .unwrap_or(false)
    });
    if !have_drops {
        return Ok(());
    }

    // Make sure pg_event_trigger_dropped_objects only works when running these
    // triggers. Reset `in_sql_drop` on both success and failure.
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            st.in_sql_drop = true;
        }
    });

    let res = fire_seams::event_trigger_fire::call(parsetree, EventTriggerEvent::SqlDrop, "sql_drop");

    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            st.in_sql_drop = false;
        }
    });

    res
}

// ===========================================================================
// Command collection (event_trigger.c).
// ===========================================================================

/// Whether the current state collects commands (`currentEventTriggerState` set
/// and collection not inhibited).
fn collecting() -> bool {
    CURRENT_STATE.with(|s| {
        s.borrow()
            .last()
            .map(|st| !st.command_collection_inhibited)
            .unwrap_or(false)
    })
}

/// `EventTriggerCollectSimpleCommand` (event_trigger.c) — collect a simple DDL
/// command so `ddl_command_end` triggers can reach it.
///
/// The parse tree is deep copied into the owning state's private `cmd_cxt`
/// arena (the C `MemoryContextSwitchTo(currentEventTriggerState->cxt)` +
/// `copyObject`), then appended to `commandList`.
pub fn event_trigger_collect_simple_command(
    address: ObjectAddress,
    secondary_object: ObjectAddress,
    parsetree: &Node,
) -> PgResult<()> {
    // Ignore if event trigger context not set, or collection disabled.
    if !collecting() {
        return Ok(());
    }

    let in_extension = extension_seams::creating_extension::call();

    CURRENT_STATE.with(|s| {
        let mut stack = s.borrow_mut();
        let st = match stack.last_mut() {
            Some(st) => st,
            None => return Ok(()),
        };

        // copyObject into the state's private arena.
        let copied = parsetree.clone_in(st.cmd_cxt.mcx())?;
        // SAFETY: `copied` lives in `cmd_cxt` (real owned heap freed by its own
        // `PgBox`/`PgVec` `Drop`). `command_list` is dropped before `cmd_cxt`
        // (field declaration order in `EventTriggerQueryState`), so the copy
        // never outlives the arena it deallocates through. The `'static` is the
        // arena-lived marker, mirroring portalmem's `copy_stmts_into_portal_context`.
        let copied: Node<'static> =
            unsafe { core::mem::transmute::<Node<'_>, Node<'static>>(copied) };

        let command = CollectedCommand {
            in_extension,
            parsetree: copied,
            address,
            secondary_object,
        };

        st.command_list
            .try_reserve(1)
            .map_err(|_| st.cmd_cxt.oom(core::mem::size_of::<CollectedCommand>()))?;
        st.command_list.push(command);
        Ok(())
    })
}

// ===========================================================================
// Object-type support tables (event_trigger.c).
// ===========================================================================

/// `EventTriggerSupportsObjectType` (event_trigger.c) — does this object type
/// have event-trigger support? Routes the GRANT / DROP / RENAME / ALTER /
/// COMMENT / SECURITY LABEL dispatch arms to `ProcessUtilitySlow` (when true)
/// or the direct handler.
pub fn event_trigger_supports_object_type(obtype: ObjectType) -> bool {
    match obtype {
        // No support for global objects (except subscriptions).
        ObjectType::Database
        | ObjectType::Tablespace
        | ObjectType::Role
        | ObjectType::ParameterAcl => false,
        // No support for event triggers on event triggers.
        ObjectType::EventTrigger => false,
        _ => true,
    }
}

/// `EventTriggerSupportsObject` (event_trigger.c) — does the event-trigger
/// facility support this object class? (dependency.c's `sql_drop` collection
/// gate.)
pub fn event_trigger_supports_object(object: &ObjectAddress) -> bool {
    match object.classId {
        // No support for global objects (except subscriptions).
        DATABASE_RELATION_ID
        | TABLE_SPACE_RELATION_ID
        | AUTH_ID_RELATION_ID
        | AUTH_MEM_RELATION_ID
        | PARAMETER_ACL_RELATION_ID => false,
        // No support for event triggers on event triggers.
        EVENT_TRIGGER_RELATION_ID => false,
        _ => true,
    }
}

// ===========================================================================
// Seam install.
// ===========================================================================

/// Install this unit's fence entry points into the consumers' seam tables.
pub fn init_seams() {
    backend_tcop_utility_out_seams::event_trigger_begin_complete_query::set(
        event_trigger_begin_complete_query,
    );
    backend_tcop_utility_out_seams::event_trigger_end_complete_query::set(
        event_trigger_end_complete_query,
    );
    backend_tcop_utility_out_seams::event_trigger_ddl_command_start::set(|parsetree| {
        event_trigger_ddl_command_start(parsetree)
    });
    backend_tcop_utility_out_seams::event_trigger_ddl_command_end::set(|parsetree| {
        event_trigger_ddl_command_end(parsetree)
    });
    backend_tcop_utility_out_seams::event_trigger_sql_drop::set(|parsetree| {
        event_trigger_sql_drop(parsetree)
    });
    backend_tcop_utility_out_seams::event_trigger_collect_simple_command::set(
        event_trigger_collect_simple_command,
    );
    backend_tcop_utility_out_seams::event_trigger_supports_object_type::set(
        event_trigger_supports_object_type,
    );

    // Inward seams (callers: tcop/utility dispatch via the out-seams above; and
    // catalog/dependency.c's drop-time `sql_drop` collection gate). Only the
    // pure-table / cache-lookup members of this unit's seam crate land here; the
    // catalog-heavy `EventTriggerSQLDropAddObject` body and the
    // CREATE/ALTER/owner collection routines are the deeper sub-campaign and
    // stay uninstalled (unreachable in standalone, where
    // `trackDroppedObjectsNeeded` is false so the drop loop never calls them).
    backend_commands_event_trigger_seams::trackDroppedObjectsNeeded::set(
        track_dropped_objects_needed,
    );
    backend_commands_event_trigger_seams::EventTriggerSupportsObject::set(|object| {
        Ok(event_trigger_supports_object(object))
    });
}

