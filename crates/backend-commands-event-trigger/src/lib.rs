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

/// `EventTriggerInhibitCommandCollection` (event_trigger.c) — suppress DDL
/// command collection for the duration of a command that should not be
/// reported (e.g. REFRESH MATERIALIZED VIEW CONCURRENTLY's internal work).
///
/// C: `if (!currentEventTriggerState) return; currentEventTriggerState->
/// commandCollectionInhibited = true;`. When no query state is installed
/// (the common no-event-trigger case) this is a cheap no-op.
pub fn event_trigger_inhibit_command_collection() {
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            st.command_collection_inhibited = true;
        }
    });
}

/// `EventTriggerUndoInhibitCommandCollection` (event_trigger.c) — re-establish
/// DDL command collection after a previously inhibited stretch.
///
/// C: `if (!currentEventTriggerState) return; currentEventTriggerState->
/// commandCollectionInhibited = false;`. A no-op when no query state is set.
pub fn event_trigger_undo_inhibit_command_collection() {
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            st.command_collection_inhibited = false;
        }
    });
}

/// Process-global backing for the `event_triggers` GUC.
///
/// C: `bool event_triggers = true;` (event_trigger.c:86) is the
/// `conf->variable` for the `event_triggers` entry in guc_tables.c. This unit
/// owns that global, so it owns its backing store here and installs the
/// matching [`GucVarAccessors`](backend_utils_misc_guc_tables::GucVarAccessors)
/// into the GUC engine's [`vars::event_triggers`] slot from [`init_seams`].
/// Seeded to the C `boot_val` (`true`).
mod gucs {
    use std::sync::atomic::{AtomicBool, Ordering};

    static EVENT_TRIGGERS: AtomicBool = AtomicBool::new(true);

    pub fn event_triggers() -> bool {
        EVENT_TRIGGERS.load(Ordering::Relaxed)
    }

    pub fn set_event_triggers(v: bool) {
        EVENT_TRIGGERS.store(v, Ordering::Relaxed);
    }
}

// ===========================================================================
// Firing fences (event_trigger.c) — gated off in standalone single-user mode.
// ===========================================================================

/// Event triggers are completely disabled in standalone mode and behind the
/// `event_triggers` GUC (see `EventTriggerDDLCommandStart`'s comment in C).
fn event_triggers_active() -> bool {
    init_small_seams::is_under_postmaster::call() && vars::event_triggers.read()
}

/// `EventTriggerCommonSetup` (event_trigger.c) — the fast-exit head: look up the
/// event cache and decide whether any trigger could possibly fire.
///
/// In C the function returns the OID run-list (`NIL` if empty). Here we split
/// it at the cache lookup: if `EventCacheLookup(event)` is empty there is no run
/// list and the caller early-returns (the fresh-cluster / type-tests case — no
/// `pg_event_trigger` rows). Only when the cache is non-empty do we cross
/// [`fire_seams::event_trigger_fire`], which carries the rest of
/// `EventTriggerCommonSetup` (the `CreateCommandTag` + `filter_event_trigger`
/// run-list build) and `EventTriggerInvoke` (the fmgr dispatch). That tail is
/// the deeper firing sub-campaign and stays a loud panic until ported —
/// unreachable until an event trigger actually exists.
fn event_trigger_cache_nonempty(event: EventTriggerEvent) -> PgResult<bool> {
    // C runs `EventCacheLookup` in `CurrentMemoryContext`; the result is only
    // inspected for emptiness here, so charge it to a transient context that is
    // freed on return (the run-list copy into the query context happens inside
    // the firing tail, behind the seam, only when non-empty).
    let cxt = MemoryContext::new("event trigger common-setup lookup");
    let any = !evtcache_seams::event_cache_lookup::call(cxt.mcx(), event)?.is_empty();
    Ok(any)
}

/// `EventTriggerDDLCommandStart` (event_trigger.c) — fire ddl_command_start
/// triggers.
pub fn event_trigger_ddl_command_start(parsetree: &Node) -> PgResult<()> {
    if !event_triggers_active() {
        return Ok(());
    }
    // EventTriggerCommonSetup fast-exit: no triggers in the cache → NIL run-list
    // → return. The firing tail is unreached on a fresh cluster.
    if !event_trigger_cache_nonempty(EventTriggerEvent::DdlCommandStart)? {
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
    // EventTriggerCommonSetup fast-exit (see ddl_command_start).
    if !event_trigger_cache_nonempty(EventTriggerEvent::DdlCommandEnd)? {
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

    // EventTriggerCommonSetup fast-exit (see ddl_command_start): if no sql_drop
    // trigger is in the cache the run-list is NIL and nothing fires.
    if !event_trigger_cache_nonempty(EventTriggerEvent::SqlDrop)? {
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

/// `EventTriggerCollectSimpleCommand(address, secondaryObject, (Node *) stmt)`
/// (event_trigger.c) for a CREATE SCHEMA `CreateSchemaStmt` (schemacmds.c
/// `CreateSchemaCommand`). No-op without an active collection state (the
/// standalone / no-trigger case), matching the C early `return`. The active
/// path deep-copies the statement into the state's private `cmd_cxt` arena and
/// appends a `CollectedCommand`, exactly like the `Node`-typed
/// [`event_trigger_collect_simple_command`] above (a `CreateSchemaStmt` is an
/// ordinary parse-tree node, so this path is fully modelled).
pub fn event_trigger_collect_simple_command_create_schema(
    address: ObjectAddress,
    secondary_object: ObjectAddress,
    stmt: &types_nodes::ddlnodes::CreateSchemaStmt<'_>,
) -> PgResult<()> {
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

        // copyObject into the state's private arena, wrapped as a Node.
        let copied_stmt = stmt.clone_in(st.cmd_cxt.mcx())?;
        let copied = Node::mk_create_schema_stmt(st.cmd_cxt.mcx(), copied_stmt);
        // SAFETY: `copied` lives in `cmd_cxt`; `command_list` is dropped before
        // `cmd_cxt` (field order in `EventTriggerQueryState`), so the copy never
        // outlives its arena — same invariant as the generic collector above.
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

/// `EventTriggerCollectSimpleCommand` (event_trigger.c) for a CREATE OPERATOR
/// FAMILY `CreateOpFamilyStmt` (opclasscmds.c `CreateOpFamily`). No-op without
/// an active collection state (the standalone / no-trigger case), matching the
/// C early `return`. The active path would deep-copy the statement into the
/// state arena like its `Node`-typed sibling above; that command-deparse path
/// is the deeper sub-campaign and is not modelled here, so it loudly stops.
pub fn event_trigger_collect_simple_command_opfamily(
    _address: ObjectAddress,
    _secondary_object: ObjectAddress,
    _stmt: &types_opclass::CreateOpFamilyStmt,
) -> PgResult<()> {
    if !collecting() {
        return Ok(());
    }
    panic!(
        "EventTriggerCollectSimpleCommand: CREATE OPERATOR FAMILY command \
         collection is part of the command-deparse sub-campaign and is not \
         modelled here"
    );
}

/// `EventTriggerCollectCreateOpClass(stmt, opclassoid, operators, procedures)`
/// (event_trigger.c) — record a CREATE OPERATOR CLASS. No-op without an active
/// collection state (standalone). The active path builds an `SCT_CreateOpClass`
/// `CollectedCommand` carrying the `operators`/`procedures` member lists, a
/// command variant that is part of the command-deparse sub-campaign and is not
/// modelled here.
pub fn event_trigger_collect_create_opclass(
    _stmt: &types_opclass::CreateOpClassStmt,
    _opclassoid: Oid,
    _operators: &[types_opclass::OpFamilyMember],
    _procedures: &[types_opclass::OpFamilyMember],
) -> PgResult<()> {
    if !collecting() {
        return Ok(());
    }
    panic!(
        "EventTriggerCollectCreateOpClass: the SCT_CreateOpClass collected \
         command (with its operator/procedure member lists) is part of the \
         command-deparse sub-campaign and is not modelled here"
    );
}

/// `EventTriggerCollectAlterOpFam(stmt, opfamilyoid, operators, procedures)`
/// (event_trigger.c) — record an ALTER OPERATOR FAMILY ADD/DROP. No-op without
/// an active collection state (standalone). The active path builds an
/// `SCT_AlterOpFamily` `CollectedCommand`, part of the unmodelled
/// command-deparse sub-campaign.
pub fn event_trigger_collect_alter_opfam(
    _stmt: &types_opclass::AlterOpFamilyStmt,
    _opfamilyoid: Oid,
    _operators: &[types_opclass::OpFamilyMember],
    _procedures: &[types_opclass::OpFamilyMember],
) -> PgResult<()> {
    if !collecting() {
        return Ok(());
    }
    panic!(
        "EventTriggerCollectAlterOpFam: the SCT_AlterOpFamily collected command \
         is part of the command-deparse sub-campaign and is not modelled here"
    );
}

/// `EventTriggerAlterTableStart(parsetree)` (event_trigger.c:1753) — begin
/// collecting an ALTER TABLE command. When no event-trigger state is set or
/// collection is inhibited (the standalone / no-trigger case), this is a no-op,
/// matching the C early `return`. The active-collection path pushes a fresh
/// `SCT_AlterTable` `CollectedCommand` onto the `currentCommand` stack; that
/// command variant (with its `parent` back-pointer and `alterTable.subcmds`
/// list) is part of the deeper command-deparse sub-campaign and is not modelled
/// by this crate's simple-command `CollectedCommand`, so it loudly stops.
pub fn event_trigger_alter_table_start(_parsetree: &Node) {
    // if (!currentEventTriggerState || commandCollectionInhibited) return;
    if !collecting() {
        return;
    }
    panic!(
        "EventTriggerAlterTableStart: SCT_AlterTable command collection (the \
         currentCommand stack + alterTable.subcmds list) is part of the \
         command-deparse sub-campaign and is not modelled here"
    );
}

/// `EventTriggerAlterTableRelid(objectId)` (event_trigger.c:1787) — stash the
/// OID of the relation being altered on the in-progress `currentCommand`. No-op
/// without an active collection state (standalone). The active path writes
/// `currentCommand->d.alterTable.objectId`, which lives on the unmodelled
/// `SCT_AlterTable` command.
pub fn event_trigger_alter_table_relid(_object_id: Oid) {
    if !collecting() {
        return;
    }
    panic!(
        "EventTriggerAlterTableRelid: writes the unmodelled SCT_AlterTable \
         currentCommand (command-deparse sub-campaign)"
    );
}

/// `EventTriggerAlterTableEnd()` (event_trigger.c:1840) — finish collecting the
/// ALTER TABLE command and, if it gathered any subcommands, append it to the
/// command list. No-op without an active collection state (standalone).
pub fn event_trigger_alter_table_end() {
    if !collecting() {
        return;
    }
    panic!(
        "EventTriggerAlterTableEnd: pops/commits the unmodelled SCT_AlterTable \
         currentCommand (command-deparse sub-campaign)"
    );
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
    // The `event_triggers` GUC's C `conf->variable` (`bool event_triggers`,
    // event_trigger.c:86) is owned by this unit. Install its accessors over our
    // backing store so the GUC engine's `.read()`/`.write()` (and the firing
    // fences' `vars::event_triggers.read()`) reach it. Mirrors C's
    // build_guc_variables wiring `&event_triggers` at startup, before any
    // statement runs.
    vars::event_triggers.install(backend_utils_misc_guc_tables::GucVarAccessors {
        get: gucs::event_triggers,
        set: gucs::set_event_triggers,
    });

    backend_tcop_utility_out_seams::event_trigger_begin_complete_query::set(
        event_trigger_begin_complete_query,
    );
    backend_tcop_utility_out_seams::event_trigger_end_complete_query::set(
        event_trigger_end_complete_query,
    );
    backend_tcop_utility_out_seams::event_trigger_inhibit_command_collection::set(
        event_trigger_inhibit_command_collection,
    );
    backend_tcop_utility_out_seams::event_trigger_undo_inhibit_command_collection::set(
        event_trigger_undo_inhibit_command_collection,
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
    backend_tcop_utility_out_seams::event_trigger_alter_table_start::set(
        event_trigger_alter_table_start,
    );
    backend_tcop_utility_out_seams::event_trigger_alter_table_relid::set(
        event_trigger_alter_table_relid,
    );
    backend_tcop_utility_out_seams::event_trigger_alter_table_end::set(
        event_trigger_alter_table_end,
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

    // CREATE/ALTER OPERATOR CLASS/FAMILY command collection (opclasscmds.c).
    // No-ops in standalone (no event-trigger state); the active-collection
    // deparse path stops loudly inside each body.
    backend_commands_event_trigger_seams::event_trigger_collect_simple_command::set(
        event_trigger_collect_simple_command_opfamily,
    );
    backend_commands_event_trigger_seams::event_trigger_collect_simple_command_create_schema::set(
        event_trigger_collect_simple_command_create_schema,
    );
    backend_commands_event_trigger_seams::event_trigger_collect_create_opclass::set(
        event_trigger_collect_create_opclass,
    );
    backend_commands_event_trigger_seams::event_trigger_collect_alter_opfam::set(
        event_trigger_collect_alter_opfam,
    );
}

