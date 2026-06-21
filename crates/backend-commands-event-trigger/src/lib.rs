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

use mcx::{Mcx, MemoryContext};
use types_catalog::catalog::{
    AUTH_ID_RELATION_ID, AUTH_MEM_RELATION_ID, DATABASE_RELATION_ID, EVENT_TRIGGER_RELATION_ID,
    PARAMETER_ACL_RELATION_ID, PROCEDURE_RELATION_ID, RELATION_RELATION_ID, TABLE_SPACE_RELATION_ID,
};
use types_catalog::catalog_dependency::{InvalidObjectAddress, ObjectAddress};
use types_catalog::pg_event_trigger::PgEventTriggerInsertRow;
use types_core::primitive::{InvalidOid, Oid};
use types_error::{
    PgError, PgResult, ERROR, ERRCODE_DUPLICATE_OBJECT,
    ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_OBJECT,
};
use types_evtcache::EventTriggerEvent;
use types_nodes::nodes::Node;
use types_nodes::parsenodes::{ObjectType, OBJECT_EVENT_TRIGGER};

use backend_commands_event_trigger_fire_seams as fire_seams;
use backend_commands_extension_seams as extension_seams;
use backend_utils_cache_evtcache_seams as evtcache_seams;
use backend_utils_init_small_seams as init_small_seams;
use backend_utils_misc_guc_tables::vars;

use backend_catalog_aclchk_seams as aclchk_seams;
use backend_catalog_indexing_seams as indexing_seams;
use backend_catalog_objectaccess_seams as objectaccess_seams;
use backend_utils_adt_arrayfuncs_seams as arrayfuncs_seams;
use backend_utils_adt_varlena_seams as varlena_seams;
use backend_utils_cache_syscache_seams as syscache_seams;
use backend_utils_error::ereport;
use backend_utils_fmgr_funcapi_seams as funcapi_seams;

use types_nodes::parsestmt::CommandTag;

/// `TRIGGER_FIRES_ON_ORIGIN` `'O'` (utils/rel.h) — the on-disk `evtenabled`
/// firing-configuration byte set at creation.
const TRIGGER_FIRES_ON_ORIGIN: i8 = b'O' as i8;
/// `TRIGGER_DISABLED` `'D'` (utils/rel.h).
const TRIGGER_DISABLED: i8 = b'D' as i8;
/// `EVENT_TRIGGEROID` (pg_type) — the `event_trigger` pseudo-type a handler
/// function must return.
const EVENT_TRIGGEROID: Oid = 3838;

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
    /// The `command->type`-specific payload (deparse_utility.h `union`).
    /// `Simple` covers every `SCT_*` simple form modelled by this crate;
    /// `AlterTable` carries the `d.alterTable` fields built by
    /// [`event_trigger_alter_table_start`].
    data: CollectedCommandData,
}

/// The `command->d` union of `CollectedCommand` (deparse_utility.h). Only the
/// two variants this crate needs are modelled: the simple-command default and
/// the `SCT_AlterTable` form with its `subcmds` list.
#[allow(dead_code)]
enum CollectedCommandData {
    /// Any `SCT_Simple` / `SCT_AlterDefaultPrivileges` / etc. — the parse tree
    /// on the enclosing [`CollectedCommand`] is the whole record.
    Simple,
    /// `command->d.alterTable` (`type == SCT_AlterTable`).
    AlterTable {
        /// `classId` — always `RelationRelationId` in the C path.
        class_id: Oid,
        /// `objectId` — the altered relation, stashed by
        /// [`event_trigger_alter_table_relid`].
        object_id: Oid,
        /// `subcmds` — list of [`CollectedATSubcmd`] gathered by
        /// [`event_trigger_collect_alter_table_subcmd`].
        subcmds: Vec<CollectedATSubcmd>,
    },
}

/// `typedef struct CollectedATSubcmd` (deparse_utility.h) — one ALTER TABLE
/// subcommand collected under an active event-trigger state. Both fields are
/// arena-lived (the subcmd parse tree is deep-copied into the state's private
/// `cmd_cxt`, like every other collected parse tree).
#[allow(dead_code)]
struct CollectedATSubcmd {
    address: ObjectAddress,
    parsetree: Node<'static>,
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
    /// `currentCommand` — the in-progress nestable command (currently only the
    /// `SCT_AlterTable` form). C keeps a single pointer with a `parent`
    /// back-link; the equivalent stack is this `Vec` whose top is
    /// `currentCommand` and whose predecessors are reached via `->parent`.
    /// Pushed by [`event_trigger_alter_table_start`], popped (and conditionally
    /// promoted into `command_list`) by [`event_trigger_alter_table_end`].
    current_command: Vec<CollectedCommand>,

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
        current_command: Vec::new(),
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

/// `EventTriggerSQLDropAddObject(object, original, normal)` (event_trigger.c) —
/// record a dropped object in the current `sql_drop` collection state, so
/// `pg_event_trigger_dropped_objects` can later report it. Called from
/// `deleteObjectsInList` (dependency.c) for every dropped object when a
/// `sql_drop` (or `ddl_command_end`) trigger / affected-object statistics make
/// `trackDroppedObjectsNeeded()` true.
///
/// No-op without an active state (`currentEventTriggerState == NULL`). The
/// descriptive-field computation (`obtain_object_name_namespace` +
/// `getObjectIdentityParts` + `getObjectTypeDescription`) runs in
/// `backend-catalog-objectaddress` via the `event_trigger_describe_dropped_object`
/// seam (where the `ObjectProperty` / identity machinery lives); this function
/// owns only the list append, in the state's private `cmd_cxt` arena (the C
/// `MemoryContextSwitchTo(currentEventTriggerState->cxt)`).
pub fn event_trigger_sql_drop_add_object(
    object: &ObjectAddress,
    original: bool,
    normal: bool,
) -> PgResult<()> {
    if !state_is_set() {
        return Ok(());
    }

    // Assert(EventTriggerSupportsObject(object)); — the dependency.c caller
    // already gates on this, but mirror the C contract defensively.
    debug_assert!(event_trigger_supports_object(object));

    // Compute the descriptive fields where the catalog machinery lives. This is
    // charged to a transient scratch context (the strings are copied into owned
    // `String`s before it is dropped); the C does this work in `state->cxt`, but
    // since the carrier is owned (`String`s, not arena pointers) the scratch
    // context's lifetime is irrelevant to the stored result.
    let scratch = MemoryContext::new("EventTriggerSQLDropAddObject describe");
    let info = backend_catalog_objectaddress_seams::event_trigger_describe_dropped_object::call(
        scratch.mcx(),
        object,
    )?;

    // `obtain_object_name_namespace` decided this object is not ours (a foreign
    // temp object): record nothing (the C `pfree(obj); return;`).
    if !info.report {
        return Ok(());
    }

    let obj = SQLDropObject {
        address: *object,
        schemaname: info.schemaname,
        objname: info.objname,
        objidentity: info.objidentity,
        objecttype: info.objecttype,
        addrnames: info.addrnames,
        addrargs: info.addrargs,
        original,
        normal,
        istemp: info.istemp,
    };

    CURRENT_STATE.with(|s| {
        let mut stack = s.borrow_mut();
        let st = match stack.last_mut() {
            Some(st) => st,
            None => return Ok(()),
        };
        // slist_push_head(&SQLDropList, &obj->next): C prepends, so the list is
        // in reverse-insertion order; `pg_event_trigger_dropped_objects`
        // `slist_foreach`es it in that same head-first order. We push to the
        // front of the Vec to match the reported order exactly.
        st.sql_drop_list
            .try_reserve(1)
            .map_err(|_| st.cmd_cxt.oom(core::mem::size_of::<SQLDropObject>()))?;
        st.sql_drop_list.insert(0, obj);
        Ok(())
    })
}

/// `pg_event_trigger_dropped_objects()` (event_trigger.c) — the `sql_drop`
/// event-trigger SRF that returns one row per object the current command
/// dropped, reading `currentEventTriggerState->SQLDropList`. Faithful PG 18.3
/// 12-column layout: `classid`, `objid`, `objsubid`, `original`, `normal`,
/// `is_temporary`, `object_type`, `schema_name`, `object_name`,
/// `object_identity`, `address_names`, `address_args`.
///
/// `mcx` is the per-query context the C function's `CStringGetTextDatum` /
/// `strlist_to_textarray` palloc in (the fmgr adapter passes `fcinfo->fn_mcxt`).
pub fn pg_event_trigger_dropped_objects<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut types_nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    use types_tuple::backend_access_common_heaptuple::Datum as DatumV;

    const PG_EVENT_TRIGGER_DROPPED_OBJECTS_COLS: usize = 12;

    // Protect this function from being called out of context.
    let ok = CURRENT_STATE.with(|s| {
        s.borrow()
            .last()
            .map(|st| st.in_sql_drop)
            .unwrap_or(false)
    });
    if !ok {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED)
            .errmsg(
                "pg_event_trigger_dropped_objects() can only be called in a sql_drop event trigger function"
                    .to_string(),
            )
            .into_error());
    }

    // Build tuplestore to hold the result rows.
    funcapi_seams::InitMaterializedSRF::call(fcinfo, 0)?;

    // Snapshot the rows we need to report. `slist_foreach` over the C
    // head-pushed `SQLDropList` walks it in head-first order; our `sql_drop_list`
    // is already maintained in that same order (front insertion), so a forward
    // iteration reproduces the C report order. We clone the descriptive fields
    // out of the thread-local so the borrow is released before we re-enter the
    // tuplestore append (which can `ereport`).
    let rows: Vec<SQLDropObject> = CURRENT_STATE.with(|s| {
        s.borrow()
            .last()
            .map(|st| st.sql_drop_list.clone())
            .unwrap_or_default()
    });

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("InitMaterializedSRF establishes fcinfo->resultinfo");

    for obj in &rows {
        let mut values: [DatumV<'mcx>; PG_EVENT_TRIGGER_DROPPED_OBJECTS_COLS] =
            core::array::from_fn(|_| DatumV::null());
        let mut nulls = [false; PG_EVENT_TRIGGER_DROPPED_OBJECTS_COLS];

        // classid / objid / objsubid
        values[0] = DatumV::from_oid(obj.address.classId);
        values[1] = DatumV::from_oid(obj.address.objectId);
        values[2] = DatumV::from_i32(obj.address.objectSubId);
        // original / normal / is_temporary
        values[3] = DatumV::from_bool(obj.original);
        values[4] = DatumV::from_bool(obj.normal);
        values[5] = DatumV::from_bool(obj.istemp);

        // object_type (NOT NULL: getObjectTypeDescription(false))
        match &obj.objecttype {
            Some(t) => values[6] = varlena_seams::cstring_to_text_v::call(mcx, t)?,
            None => nulls[6] = true,
        }
        // schema_name
        match &obj.schemaname {
            Some(s) => values[7] = varlena_seams::cstring_to_text_v::call(mcx, s)?,
            None => nulls[7] = true,
        }
        // object_name
        match &obj.objname {
            Some(s) => values[8] = varlena_seams::cstring_to_text_v::call(mcx, s)?,
            None => nulls[8] = true,
        }
        // object_identity
        match &obj.objidentity {
            Some(s) => values[9] = varlena_seams::cstring_to_text_v::call(mcx, s)?,
            None => nulls[9] = true,
        }

        // address_names / address_args (text[] columns)
        match &obj.addrnames {
            Some(names) => {
                let elems: Vec<Option<&[u8]>> =
                    names.iter().map(|s| Some(s.as_bytes())).collect();
                values[10] =
                    DatumV::ByRef(arrayfuncs_seams::build_text_array_nullable::call(mcx, &elems)?);
                // addrargs ? strlist_to_textarray(addrargs)
                //          : construct_empty_array(TEXTOID)
                let arg_elems: Vec<Option<&[u8]>> = match &obj.addrargs {
                    Some(args) => args.iter().map(|s| Some(s.as_bytes())).collect(),
                    None => Vec::new(),
                };
                values[11] = DatumV::ByRef(
                    arrayfuncs_seams::build_text_array_nullable::call(mcx, &arg_elems)?,
                );
            }
            None => {
                nulls[10] = true;
                nulls[11] = true;
            }
        }

        funcapi_seams::materialized_srf_putvalues::call(rsinfo, &values, &nulls)?;
    }

    // return (Datum) 0;
    Ok(DatumV::null())
}

// ===========================================================================
// EventTriggerCommonSetup run-list + EventTriggerInvoke (event_trigger.c) — the
// post-gate firing tail crossed by `fire_seams::event_trigger_fire`.
// ===========================================================================

/// `T_EventTriggerData` (nodetags.h) — the fmgr call-context demux tag stamped
/// on the firing call frame so the trigger-language handler's
/// `CALLED_AS_EVENT_TRIGGER(fcinfo)` fires.
const T_EVENT_TRIGGER_DATA: u32 = 443;

/// `TRIGGER_FIRES_ON_REPLICA` `'R'` (utils/rel.h) — the `evtenabled` value of a
/// trigger that fires only under `session_replication_role = replica`.
const TRIGGER_FIRES_ON_REPLICA: i8 = b'R' as i8;
/// `SESSION_REPLICATION_ROLE_REPLICA` (utils/guc.h).
const SESSION_REPLICATION_ROLE_REPLICA: i32 = 1;

/// The currently-firing event trigger's `event` / `tag` — the owned analogue of
/// `estate->evtrigdata->{event,tag}`, read by the PL/pgSQL `TG_EVENT` / `TG_TAG`
/// promises through the [`event_trigger_get_event`](backend_commands_event_trigger_seams::event_trigger_get_event)
/// / [`event_trigger_get_tag_name`](backend_commands_event_trigger_seams::event_trigger_get_tag_name)
/// accessor seams. Like the DML-trigger `LocTriggerData` side-channel in
/// `commands/trigger.c`, the rich `EventTriggerData` cannot ride the tag-only
/// fmgr `ContextNode`, so it lives here for the call's duration.
struct CurrentEventTrigger {
    /// `trigdata->event` — the event name string (`"ddl_command_start"` etc.).
    event: String,
    /// `trigdata->tag` — the command tag the firing command produced.
    tag: CommandTag,
}

thread_local! {
    /// `EventTriggerData` of the event-trigger call in flight on this backend
    /// thread, or `None` outside such a call.
    static CURRENT_EVENT_TRIGGER: RefCell<Option<CurrentEventTrigger>> =
        const { RefCell::new(None) };
}

/// RAII guard installing the current event-trigger side-channel for the firing
/// call's duration, restoring the prior value on drop (so a nested event trigger
/// firing another DDL nests correctly).
struct CurrentEventTriggerGuard {
    prev: Option<CurrentEventTrigger>,
}

impl CurrentEventTriggerGuard {
    fn install(data: CurrentEventTrigger) -> Self {
        let prev = CURRENT_EVENT_TRIGGER.with(|c| c.borrow_mut().replace(data));
        CurrentEventTriggerGuard { prev }
    }
}

impl Drop for CurrentEventTriggerGuard {
    fn drop(&mut self) {
        CURRENT_EVENT_TRIGGER.with(|c| *c.borrow_mut() = self.prev.take());
    }
}

/// `estate->evtrigdata->event` — the firing event's name, or `None` outside an
/// event-trigger call (the C `evtrigdata == NULL` guard).
pub fn event_trigger_get_event() -> Option<String> {
    CURRENT_EVENT_TRIGGER.with(|c| c.borrow().as_ref().map(|d| d.event.clone()))
}

/// `GetCommandTagName(estate->evtrigdata->tag)` — the firing command's tag name,
/// or `None` outside an event-trigger call.
pub fn event_trigger_get_tag_name() -> Option<String> {
    CURRENT_EVENT_TRIGGER.with(|c| {
        c.borrow()
            .as_ref()
            .map(|d| backend_tcop_cmdtag::get_command_tag_name(d.tag.0).to_string())
    })
}

/// `EventTriggerGetTag(parsetree, event)` (event_trigger.c) — the command tag for
/// the firing command. `EVT_Login` is `CMDTAG_LOGIN`; everything else is
/// `CreateCommandTag(parsetree)`.
fn event_trigger_get_tag(parsetree: &Node, event: EventTriggerEvent) -> PgResult<CommandTag> {
    if event == EventTriggerEvent::Login {
        // CMDTAG_LOGIN (cmdtaglist.h) — resolve its tag index by name.
        Ok(CommandTag(backend_tcop_cmdtag::get_command_tag_enum(b"LOGIN")))
    } else {
        backend_tcop_utility_seams::create_command_tag::call(parsetree)
    }
}

/// `filter_event_trigger(tag, item)` (event_trigger.c) — should this cached
/// trigger fire for this command tag? Filters by session replication role and by
/// the trigger's tag set (when specified).
fn filter_event_trigger(
    tag: CommandTag,
    item: &types_evtcache::EventTriggerCacheItem<'_>,
) -> bool {
    // Filter by session replication role (we never see disabled items here).
    let session_replica = backend_utils_misc_guc_tables::vars::SessionReplicationRole.read()
        == SESSION_REPLICATION_ROLE_REPLICA;
    if session_replica {
        if item.enabled == TRIGGER_FIRES_ON_ORIGIN {
            return false;
        }
    } else if item.enabled == TRIGGER_FIRES_ON_REPLICA {
        return false;
    }

    // Filter by tags, if any were specified.
    let tagset = item.tagset.as_deref();
    if !backend_nodes_core::bitmapset::bms_is_empty(tagset)
        && !backend_nodes_core::bitmapset::bms_is_member(tag.0, tagset)
    {
        return false;
    }

    true
}

/// `EventTriggerInvoke(fn_oid_list, trigdata)` (event_trigger.c) — call each
/// event-trigger function in turn via `fmgr`.
///
/// The C per-call `MemoryContext`/`MemoryContextReset` for leak containment is
/// Rust `Drop`; the EXPLAIN `instr`/pgstat usage is not ported. The rich
/// `EventTriggerData` (event / tag / parsetree) rides the
/// [`CURRENT_EVENT_TRIGGER`] side-channel for the call's duration, with the
/// `T_EventTriggerData` demux tag stamped on the fmgr call frame so the
/// trigger-language handler's `CALLED_AS_EVENT_TRIGGER` fires.
fn event_trigger_invoke(
    fn_oid_list: &[Oid],
    event: &str,
    tag: CommandTag,
) -> PgResult<()> {
    // Guard against stack overflow due to recursive event trigger.
    backend_tcop_utility_out_seams::check_stack_depth::call()?;

    let mut first = true;
    for &fnoid in fn_oid_list {
        // Each event trigger sees the results of the previous one's action.
        if first {
            first = false;
        } else {
            backend_access_transam_xact_seams::command_counter_increment::call()?;
        }

        // Install the per-call EventTriggerData side-channel + the fmgr
        // call-context demux tag, then invoke the function with no arguments and
        // the InvalidOid collation (an event-trigger function takes no args).
        let _data_guard = CurrentEventTriggerGuard::install(CurrentEventTrigger {
            event: event.to_string(),
            tag,
        });
        let _ctx_guard = types_fmgr::fmgr::CallContextTagGuard::install(T_EVENT_TRIGGER_DATA);
        backend_utils_fmgr_fmgr_seams::function_call_invoke::call(fnoid, InvalidOid, &[])?;
    }

    Ok(())
}

/// `EventTriggerCommonSetup` run-list build + `EventTriggerInvoke` + the
/// post-fire `CommandCounterIncrement` — the firing tail crossed by
/// [`fire_seams::event_trigger_fire`]. Entered only after the fence verified the
/// event cache is non-empty for this event.
///
/// Mirrors the C `EventTriggerDDLCommandStart` tail: build the run-list (filter
/// the cache items by `filter_event_trigger`), fast-exit if empty (the common
/// trigger-present-but-tag-mismatch no-op), invoke each matching function, then
/// `CommandCounterIncrement` so the main command sees what the triggers did.
fn event_trigger_fire_impl(
    parsetree: &Node,
    event: EventTriggerEvent,
    eventstr: &str,
) -> PgResult<()> {
    // EventTriggerCommonSetup: re-read the cache (the fence's emptiness check ran
    // in a transient context) and build the run-list.
    let setup_cxt = MemoryContext::new("event trigger common-setup");
    let cachelist = evtcache_seams::event_cache_lookup::call(setup_cxt.mcx(), event)?;
    if cachelist.is_empty() {
        return Ok(());
    }

    // Get the command tag.
    let tag = event_trigger_get_tag(parsetree, event)?;

    // Filter list of event triggers by command tag; collect the fnoids to run.
    let mut runlist: Vec<Oid> = Vec::new();
    for item in cachelist.iter() {
        if filter_event_trigger(tag, item) {
            runlist.push(item.fnoid);
        }
    }

    // Don't spend any more time on this if no functions to run.
    if runlist.is_empty() {
        return Ok(());
    }

    // Run the triggers.
    event_trigger_invoke(&runlist, eventstr, tag)?;

    // Make sure anything the event triggers did is visible to the main command.
    backend_access_transam_xact_seams::command_counter_increment::call()?;

    Ok(())
}

/// `EventTriggerCommonSetup(NULL, EVT_Login, "login", &trigdata, ...)` reduced
/// to the login case — build the run-list of login event-trigger function OIDs.
/// `EVT_Login`'s tag is `CMDTAG_LOGIN` (no parse tree is consulted), so the
/// `parsetree == NULL` C call is faithful. Returns `(runlist, tag)`.
fn event_trigger_login_setup(mcx: Mcx<'_>) -> PgResult<(Vec<Oid>, CommandTag)> {
    let cachelist = evtcache_seams::event_cache_lookup::call(mcx, EventTriggerEvent::Login)?;

    // EventTriggerGetTag(NULL, EVT_Login) == CMDTAG_LOGIN.
    let tag = CommandTag(backend_tcop_cmdtag::get_command_tag_enum(b"LOGIN"));

    let mut runlist: Vec<Oid> = Vec::new();
    for item in cachelist.iter() {
        if filter_event_trigger(tag, item) {
            runlist.push(item.fnoid);
        }
    }
    Ok((runlist, tag))
}

/// `EventTriggerOnLogin()` (event_trigger.c:896-996) — fire login event triggers
/// at connection start, if any are present.
///
/// Mirrors the C driver: fast-exit gate (`!IsUnderPostmaster || !event_triggers
/// || !OidIsValid(MyDatabaseId) || !MyDatabaseHasLoginEventTriggers`), then a
/// fresh transaction in which the run-list is built and invoked (under an active
/// transaction snapshot). When the run-list comes back empty — the
/// `dathasloginevt` flag is stale because every login trigger was dropped — it
/// conditionally takes the shared lock, rechecks, and clears the flag in place.
pub fn EventTriggerOnLogin() -> PgResult<()> {
    // See EventTriggerDDLCommandStart for why event triggers are disabled in
    // single-user mode / via GUC; we also need a valid database connection.
    if !init_small_seams::is_under_postmaster::call()
        || !vars::event_triggers.read()
        || !types_core::primitive::OidIsValid(
            backend_commands_tablespace_globals_seams::MyDatabaseId::call()?,
        )
        || !init_small_seams::my_database_has_login_event_triggers::call()
    {
        return Ok(());
    }

    backend_access_transam_xact_seams::start_transaction_command::call()?;

    // EventTriggerCommonSetup(NULL, EVT_Login, "login", &trigdata, false).
    let setup_cxt = MemoryContext::new("event trigger login-setup");
    let (runlist, tag) = event_trigger_login_setup(setup_cxt.mcx())?;

    if !runlist.is_empty() {
        // Event trigger execution may require an active snapshot.
        backend_utils_time_snapmgr_seams::push_active_snapshot_transaction::call()?;

        // Run the triggers.
        event_trigger_invoke(&runlist, "login", tag)?;

        backend_utils_time_snapmgr_seams::pop_active_snapshot::call()?;
    } else {
        // No active login event trigger, but pg_database.dathasloginevt is set.
        // Try to unset the flag. Use the lock to prevent a concurrent
        // SetDatabaseHasLoginEventTriggers(), but don't block the connection —
        // acquire it conditionally.
        let my_database_id = backend_commands_tablespace_globals_seams::MyDatabaseId::call()?;
        let got = backend_storage_lmgr_lmgr_seams::conditional_lock_shared_object::call(
            types_catalog::catalog::DATABASE_RELATION_ID,
            my_database_id,
            0,
            types_storage::lock::AccessExclusiveLock,
        )?;
        if got {
            // The lock is held. Recheck that the login event trigger list is
            // still empty. Once empty, even a concurrent backend inserting /
            // enabling a login trigger will update dathasloginevt *afterwards*.
            let recheck_cxt = MemoryContext::new("event trigger login-recheck");
            let (recheck, _tag) = event_trigger_login_setup(recheck_cxt.mcx())?;
            if recheck.is_empty() {
                // table_open + 3-phase in-place clear of dathasloginevt + close,
                // owned by the pg-database unit.
                backend_catalog_pg_database_seams::reset_database_has_login_event_triggers::call(
                    setup_cxt.mcx(),
                )?;
            }
        }
    }

    backend_access_transam_xact_seams::commit_transaction_command::call()?;
    Ok(())
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
            data: CollectedCommandData::Simple,
        };

        st.command_list
            .try_reserve(1)
            .map_err(|_| st.cmd_cxt.oom(core::mem::size_of::<CollectedCommand>()))?;
        st.command_list.push(command);
        Ok(())
    })
}

/// `EventTriggerCollectAlterDefPrivs(stmt)` (event_trigger.c) — collect an
/// ALTER DEFAULT PRIVILEGES command so `ddl_command_end` triggers can reach it.
/// No-op without an active collection state (the standalone / no-trigger case),
/// matching the C `if (currentEventTriggerState == NULL || ... inhibited)
/// return;`. The active path deep-copies the `AlterDefaultPrivilegesStmt` into
/// the state's private `cmd_cxt` arena and appends a `CollectedCommand`, exactly
/// like [`event_trigger_collect_simple_command`] (an
/// `AlterDefaultPrivilegesStmt` is an ordinary parse-tree node). The C body also
/// stashes `d.defprivs.objtype = stmt->action->objtype`, which is recoverable
/// from the stored parse tree, so no extra field is modelled.
pub fn event_trigger_collect_alter_def_privs(stmt: &Node) -> PgResult<()> {
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
        let copied = stmt.clone_in(st.cmd_cxt.mcx())?;
        // SAFETY: `copied` lives in `cmd_cxt`; `command_list` is dropped before
        // `cmd_cxt` (field order in `EventTriggerQueryState`), so the copy never
        // outlives the arena it deallocates through. See
        // [`event_trigger_collect_simple_command`].
        let copied: Node<'static> =
            unsafe { core::mem::transmute::<Node<'_>, Node<'static>>(copied) };

        let command = CollectedCommand {
            in_extension,
            parsetree: copied,
            address: InvalidObjectAddress,
            secondary_object: InvalidObjectAddress,
            data: CollectedCommandData::Simple,
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
        let copied = Node::mk_create_schema_stmt(st.cmd_cxt.mcx(), copied_stmt)?;
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
            data: CollectedCommandData::Simple,
        };

        st.command_list
            .try_reserve(1)
            .map_err(|_| st.cmd_cxt.oom(core::mem::size_of::<CollectedCommand>()))?;
        st.command_list.push(command);
        Ok(())
    })
}

/// `EventTriggerCollectSimpleCommand(address, secondaryObject, (Node *) stmt)`
/// (event_trigger.c) for a `ReindexStmt` — `reindex_index` records the REINDEX
/// command for interested event triggers when invoked from a REINDEX command
/// (`stmt != NULL`). No-op without an active collection state (the standalone /
/// no-trigger case), matching the C early `return`. The active path deep-copies
/// the statement into the state's private `cmd_cxt` arena and appends a
/// `CollectedCommand`, exactly like the `CreateSchemaStmt` collector above (a
/// `ReindexStmt` is an ordinary parse-tree node, so this path is fully modelled).
pub fn event_trigger_collect_simple_command_reindex(
    address: ObjectAddress,
    secondary_object: ObjectAddress,
    stmt: &types_nodes::ddlnodes::ReindexStmt<'_>,
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
        let copied = Node::mk_reindex_stmt(st.cmd_cxt.mcx(), copied_stmt)?;
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
            data: CollectedCommandData::Simple,
        };

        st.command_list
            .try_reserve(1)
            .map_err(|_| st.cmd_cxt.oom(core::mem::size_of::<CollectedCommand>()))?;
        st.command_list.push(command);
        Ok(())
    })
}

/// `EventTriggerCollectSimpleCommand(address, secondaryObject, (Node *) stmt)`
/// (event_trigger.c) for an `AlterPublicationStmt` — the CREATE/ALTER PUBLICATION
/// command-collection path (publicationcmds.c `PublicationAddTables` /
/// `PublicationAddSchemas` / `AlterPublicationOptions`). No-op without an active
/// collection state (the standalone / no-trigger case), matching the C early
/// `return`. The active path takes the already-`copyObject`'d statement (the
/// caller clones it into the planner/utility arena before handing it over),
/// re-copies it into the state's private `cmd_cxt` arena, wraps it as a `Node`,
/// and appends a `CollectedCommand`, exactly like the `CreateSchemaStmt` /
/// `ReindexStmt` collectors above (an `AlterPublicationStmt` is an ordinary
/// parse-tree node, so this path is fully modelled).
pub fn event_trigger_collect_simple_command_publication(
    address: ObjectAddress,
    secondary_object: ObjectAddress,
    stmt: types_nodes::ddlnodes::AlterPublicationStmt<'_>,
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
        let copied = Node::mk_alter_publication_stmt(st.cmd_cxt.mcx(), copied_stmt)?;
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
            data: CollectedCommandData::Simple,
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
pub fn event_trigger_alter_table_start(parsetree: &Node) -> PgResult<()> {
    // if (!currentEventTriggerState || commandCollectionInhibited) return;
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

        // command = palloc(...); copyObject(parsetree) into the state arena.
        let copied = parsetree.clone_in(st.cmd_cxt.mcx())?;
        // SAFETY: `copied` lives in `cmd_cxt`; the `current_command` /
        // `command_list` Vecs are both dropped before `cmd_cxt` (field
        // declaration order in `EventTriggerQueryState`), so the copy never
        // outlives the arena it deallocates through. See
        // [`event_trigger_collect_simple_command`].
        let copied: Node<'static> =
            unsafe { core::mem::transmute::<Node<'_>, Node<'static>>(copied) };

        // command->type = SCT_AlterTable;
        // command->d.alterTable.classId = RelationRelationId;
        // command->d.alterTable.objectId = InvalidOid;
        // command->d.alterTable.subcmds = NIL;
        let command = CollectedCommand {
            in_extension,
            parsetree: copied,
            address: InvalidObjectAddress,
            secondary_object: InvalidObjectAddress,
            data: CollectedCommandData::AlterTable {
                class_id: RELATION_RELATION_ID,
                object_id: InvalidOid,
                subcmds: Vec::new(),
            },
        };

        // command->parent = currentCommand; currentCommand = command;
        st.current_command
            .try_reserve(1)
            .map_err(|_| st.cmd_cxt.oom(core::mem::size_of::<CollectedCommand>()))?;
        st.current_command.push(command);
        Ok(())
    })
}

/// `EventTriggerAlterTableRelid(objectId)` (event_trigger.c:1787) — stash the
/// OID of the relation being altered on the in-progress `currentCommand`. No-op
/// without an active collection state (standalone). The active path writes
/// `currentCommand->d.alterTable.objectId`, which lives on the unmodelled
/// `SCT_AlterTable` command.
pub fn event_trigger_alter_table_relid(object_id: Oid) {
    if !collecting() {
        return;
    }

    // currentEventTriggerState->currentCommand->d.alterTable.objectId = objectId;
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            if let Some(cmd) = st.current_command.last_mut() {
                if let CollectedCommandData::AlterTable { object_id: oid, .. } = &mut cmd.data {
                    *oid = object_id;
                }
            }
        }
    });
}

/// `EventTriggerCollectAlterTableSubcmd(subcmd, address)` (event_trigger.c) —
/// record one ALTER TABLE subcommand (an `AlterTableCmd`) under the in-progress
/// `SCT_AlterTable` `currentCommand`. A no-op without an active collection state
/// or when collection is inhibited (matching the C early `return`). The active
/// path deep-copies the subcommand parse tree into the state's private `cmd_cxt`
/// arena and appends a [`CollectedATSubcmd`] to
/// `currentCommand->d.alterTable.subcmds`.
pub fn event_trigger_collect_alter_table_subcmd(
    subcmd: &Node,
    address: ObjectAddress,
) -> PgResult<()> {
    // if (!currentEventTriggerState || commandCollectionInhibited) return;
    if !collecting() {
        return Ok(());
    }

    // Assert(IsA(subcmd, AlterTableCmd));
    debug_assert_eq!(subcmd.node_tag(), types_nodes::nodes::T_AlterTableCmd);

    CURRENT_STATE.with(|s| {
        let mut stack = s.borrow_mut();
        let st = match stack.last_mut() {
            Some(st) => st,
            None => return Ok(()),
        };

        // newsub = palloc(...); newsub->parsetree = copyObject(subcmd).
        let copied = subcmd.clone_in(st.cmd_cxt.mcx())?;
        // SAFETY: arena-lived; freed when `cmd_cxt` is torn down, after the
        // `current_command` Vec holding it is dropped (field declaration order).
        let copied: Node<'static> =
            unsafe { core::mem::transmute::<Node<'_>, Node<'static>>(copied) };

        // Split the borrow so the per-field `current_command` / `cmd_cxt`
        // accesses don't alias through one `&mut st`.
        let EventTriggerQueryState {
            current_command,
            cmd_cxt,
            ..
        } = st;

        // Assert(currentCommand != NULL);
        // Assert(OidIsValid(currentCommand->d.alterTable.objectId));
        let cmd = match current_command.last_mut() {
            Some(c) => c,
            None => return Ok(()),
        };
        match &mut cmd.data {
            CollectedCommandData::AlterTable { subcmds, .. } => {
                let newsub = CollectedATSubcmd {
                    address,
                    parsetree: copied,
                };
                subcmds
                    .try_reserve(1)
                    .map_err(|_| cmd_cxt.oom(core::mem::size_of::<CollectedATSubcmd>()))?;
                subcmds.push(newsub);
            }
            CollectedCommandData::Simple => {
                // currentCommand is not an SCT_AlterTable — should not happen
                // (the C path always opens one via EventTriggerAlterTableStart
                // before collecting subcmds); ignore defensively.
            }
        }
        Ok(())
    })
}

/// `EventTriggerTableRewrite(parsetree, tableOid, reason)` (event_trigger.c:1003)
/// — fire any `table_rewrite` event triggers before a table's heap is rewritten.
/// No-op when no event-trigger collection state is set up (the standalone /
/// no-relevant-trigger case: the C `!currentEventTriggerState` early return,
/// which is *necessary* per the C comment, since `EventTriggerCommonSetup` might
/// otherwise find triggers created mid-command). The active firing path
/// (`EventTriggerCommonSetup`/`EventTriggerInvoke` + the
/// `pg_event_trigger_table_rewrite_oid` state) is part of the event-trigger
/// firing sub-campaign and stops loudly until it lands.
pub fn event_trigger_table_rewrite(
    _parsetree: Option<&Node>,
    _table_oid: Oid,
    _reason: i32,
) -> PgResult<()> {
    // if (!IsUnderPostmaster || !event_triggers) return;
    // if (!currentEventTriggerState) return;
    if !collecting() {
        return Ok(());
    }
    panic!(
        "EventTriggerTableRewrite: firing table_rewrite event triggers \
         (EventTriggerCommonSetup + EventTriggerInvoke) is part of the \
         event-trigger firing sub-campaign and is not modelled here"
    );
}

/// `EventTriggerAlterTableEnd()` (event_trigger.c:1840) — finish collecting the
/// ALTER TABLE command and, if it gathered any subcommands, append it to the
/// command list. No-op without an active collection state (standalone).
pub fn event_trigger_alter_table_end() -> PgResult<()> {
    if !collecting() {
        return Ok(());
    }

    CURRENT_STATE.with(|s| {
        let mut stack = s.borrow_mut();
        let st = match stack.last_mut() {
            Some(st) => st,
            None => return Ok(()),
        };

        // parent = currentCommand->parent; (the predecessor on the stack).
        // Pop the in-progress command; promoting it to `command_list` iff it
        // gathered at least one subcommand, else discard (the C `pfree`).
        let command = match st.current_command.pop() {
            Some(c) => c,
            None => return Ok(()),
        };

        // if (currentCommand->d.alterTable.subcmds != NIL) lappend(commandList, ...)
        let has_subcmds = match &command.data {
            CollectedCommandData::AlterTable { subcmds, .. } => !subcmds.is_empty(),
            CollectedCommandData::Simple => false,
        };

        if has_subcmds {
            st.command_list
                .try_reserve(1)
                .map_err(|_| st.cmd_cxt.oom(core::mem::size_of::<CollectedCommand>()))?;
            st.command_list.push(command);
        }
        // else: drop `command` (the C `pfree`). Its arena-lived parse tree is
        // freed when `cmd_cxt` is torn down.

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
// CreateEventTrigger / AlterEventTrigger (event_trigger.c) — catalog-write side.
// ===========================================================================

/// Outward-seam adapter for `CreateEventTrigger` (utility.c:894,
/// `T_CreateEventTrigStmt`): downcast the arena [`Node`] and run the ported
/// body. The C result `Oid` is discarded by the standard ProcessUtility arm
/// ("no event triggers on event triggers"), so the seam returns `()`.
fn create_event_trigger_seam<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<()> {
    let s = stmt.as_createeventtrigstmt().ok_or_else(|| {
        PgError::error("create_event_trigger_seam: statement is not a CreateEventTrigStmt")
    })?;
    CreateEventTrigger(mcx, s)?;
    Ok(())
}

/// Outward-seam adapter for `AlterEventTrigger` (utility.c:899,
/// `T_AlterEventTrigStmt`).
fn alter_event_trigger_seam<'mcx>(stmt: &Node<'mcx>) -> PgResult<()> {
    let ctx = MemoryContext::new("AlterEventTrigger");
    let mcx = ctx.mcx();
    let s = stmt.as_altereventtrigstmt().ok_or_else(|| {
        PgError::error("alter_event_trigger_seam: statement is not an AlterEventTrigStmt")
    })?;
    let trigname = s.trigname.as_ref().map(|n| n.as_str()).unwrap_or("");
    AlterEventTrigger(mcx, trigname, s.tgenabled)?;
    Ok(())
}

/// `CreateEventTrigger` (event_trigger.c:123-210) — create an event trigger.
pub fn CreateEventTrigger<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &types_nodes::ddlnodes::CreateEventTrigStmt<'mcx>,
) -> PgResult<Oid> {
    let evtowner: Oid = backend_utils_init_miscinit::GetUserId();

    let trigname = stmt.trigname.as_ref().map(|n| n.as_str()).unwrap_or("");
    let eventname = stmt.eventname.as_ref().map(|n| n.as_str()).unwrap_or("");

    /*
     * It would be nice to allow database owners or even regular users to do
     * this, but there are obvious privilege escalation risks which would have
     * to somehow be plugged first.
     */
    if !backend_utils_init_miscinit_seams::superuser::call(mcx)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied to create event trigger \"{trigname}\""
            ))
            .errhint("Must be superuser to create an event trigger.")
            .into_error());
    }

    /* Validate event name. */
    if eventname != "ddl_command_start"
        && eventname != "ddl_command_end"
        && eventname != "sql_drop"
        && eventname != "login"
        && eventname != "table_rewrite"
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("unrecognized event name \"{eventname}\""))
            .into_error());
    }

    /* Validate filter conditions. */
    let mut tags: Option<Vec<String>> = None;
    for lc in stmt.whenclause.iter() {
        let def = lc.as_defelem().ok_or_else(|| {
            PgError::error("CreateEventTrigger: whenclause element is not a DefElem")
        })?;
        let defname = def.defname.as_ref().map(|n| n.as_str()).unwrap_or("");
        if defname == "tag" {
            if tags.is_some() {
                error_duplicate_filter_variable(defname)?;
            }
            tags = Some(def_string_list(def)?);
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("unrecognized filter variable \"{defname}\""))
                .into_error());
        }
    }

    /* Validate tag list, if any. */
    if (eventname == "ddl_command_start"
        || eventname == "ddl_command_end"
        || eventname == "sql_drop")
        && tags.is_some()
    {
        validate_ddl_tags("tag", tags.as_deref().expect("tags is Some"))?;
    } else if eventname == "table_rewrite" && tags.is_some() {
        validate_table_rewrite_tags("tag", tags.as_deref().expect("tags is Some"))?;
    } else if eventname == "login" && tags.is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("tag filtering is not supported for login event triggers")
            .into_error());
    }

    /*
     * Give user a nice error message if an event trigger of the same name
     * already exists.
     */
    if syscache_seams::event_trigger_name_exists::call(trigname)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("event trigger \"{trigname}\" already exists"))
            .into_error());
    }

    /* Find and validate the trigger function. */
    let mut names: Vec<mcx::PgString<'mcx>> = Vec::new();
    for n in stmt.funcname.iter() {
        let s = n.as_string().ok_or_else(|| {
            PgError::error("CreateEventTrigger: funcname element is not a String node")
        })?;
        names.push(mcx::PgString::from_str_in(s.sval.as_str(), mcx)?);
    }
    let funcoid = backend_parser_func::LookupFuncName(mcx, &names, 0, &[], false)?;
    let funcrettype = backend_utils_cache_lsyscache::function::get_func_rettype(funcoid)?;
    if funcrettype != EVENT_TRIGGEROID {
        let display = names
            .iter()
            .map(|s| s.as_str().to_string())
            .collect::<Vec<_>>()
            .join(".");
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!("function {display} must return type event_trigger"))
            .into_error());
    }

    /* Insert catalog entries. */
    insert_event_trigger_tuple(mcx, trigname, eventname, evtowner, funcoid, tags.as_deref())
}

/// `validate_ddl_tags` (event_trigger.c:215-237) — validate DDL command tags.
fn validate_ddl_tags(filtervar: &str, taglist: &[String]) -> PgResult<()> {
    for tag in taglist {
        let command_tag = backend_tcop_cmdtag::get_command_tag_enum(tag.as_bytes());
        if command_tag == types_portal::CMDTAG_UNKNOWN {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!(
                    "filter value \"{tag}\" not recognized for filter variable \"{filtervar}\""
                ))
                .into_error());
        }
        if !backend_tcop_cmdtag::command_tag_event_trigger_ok(command_tag) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!("event triggers are not supported for {tag}"))
                .into_error());
        }
    }
    Ok(())
}

/// `validate_table_rewrite_tags` (event_trigger.c:242-259) — validate tags for
/// the table_rewrite event.
fn validate_table_rewrite_tags(_filtervar: &str, taglist: &[String]) -> PgResult<()> {
    for tag in taglist {
        let command_tag = backend_tcop_cmdtag::get_command_tag_enum(tag.as_bytes());
        if !backend_tcop_cmdtag::command_tag_table_rewrite_ok(command_tag) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!("event triggers are not supported for {tag}"))
                .into_error());
        }
    }
    Ok(())
}

/// `error_duplicate_filter_variable` (event_trigger.c:264-271) — always errors.
fn error_duplicate_filter_variable(defname: &str) -> PgResult<()> {
    Err(ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!(
            "filter variable \"{defname}\" specified more than once"
        ))
        .into_error())
}

/// Read a `DefElem`'s `arg` as a list of `String` node values — the
/// `WHEN tag IN ('cmd1', 'cmd2')` parser representation (`(List *) def->arg`).
fn def_string_list(def: &types_nodes::ddlnodes::DefElem<'_>) -> PgResult<Vec<String>> {
    let arg = def
        .arg
        .as_ref()
        .ok_or_else(|| PgError::error("event trigger filter variable requires a parameter"))?;
    let cells = arg
        .as_list()
        .ok_or_else(|| PgError::error("event trigger filter value must be a list of names"))?;
    let mut out = Vec::new();
    for cell in cells.iter() {
        let s = cell.as_string().ok_or_else(|| {
            PgError::error("event trigger filter value must be a list of names")
        })?;
        out.push(s.sval.as_str().to_string());
    }
    Ok(out)
}

/// `insert_event_trigger_tuple` (event_trigger.c:276-346) — insert the new
/// pg_event_trigger row and record dependencies.
fn insert_event_trigger_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    trigname: &str,
    eventname: &str,
    evt_owner: Oid,
    funcoid: Oid,
    taglist: Option<&[String]>,
) -> PgResult<Oid> {
    /* Open pg_event_trigger. */
    let tgrel = backend_access_table_table_seams::table_open::call(
        mcx,
        EVENT_TRIGGER_RELATION_ID,
        types_storage::lock::RowExclusiveLock,
    )?;

    /*
     * In the parser, a clause like WHEN tag IN ('cmd1', 'cmd2') is a List of
     * String nodes; in the catalog we store the tags as a text array, with
     * each tag ASCII-uppercased (`filter_list_to_array`).
     */
    let evttags: Option<Vec<String>> = taglist.map(filter_list_to_array);

    let row = PgEventTriggerInsertRow {
        evtname: trigname.to_string(),
        evtevent: eventname.to_string(),
        evtowner: evt_owner,
        evtfoid: funcoid,
        evtenabled: TRIGGER_FIRES_ON_ORIGIN,
        evttags,
    };

    /* Build + insert the heap tuple, returning the freshly-allocated OID. */
    let trigoid = indexing_seams::catalog_tuple_insert_pg_event_trigger::call(mcx, &tgrel, &row)?;

    /*
     * Login event triggers have an additional flag in pg_database to enable
     * faster lookups in hot codepaths. Set the flag unless already True.
     */
    if eventname == "login" {
        SetDatabaseHasLoginEventTriggers(mcx)?;
    }

    /* Depend on owner. */
    backend_catalog_pg_shdepend::recordDependencyOnOwner(
        EVENT_TRIGGER_RELATION_ID,
        trigoid,
        evt_owner,
    )?;

    /* Depend on event trigger function. */
    let myself = ObjectAddress {
        classId: EVENT_TRIGGER_RELATION_ID,
        objectId: trigoid,
        objectSubId: 0,
    };
    let referenced = ObjectAddress {
        classId: PROCEDURE_RELATION_ID,
        objectId: funcoid,
        objectSubId: 0,
    };
    backend_catalog_pg_depend::recordDependencyOn(
        mcx,
        &myself,
        &referenced,
        types_catalog::catalog_dependency::DEPENDENCY_NORMAL,
    )?;

    /* Depend on extension, if any. */
    backend_catalog_pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, false)?;

    /* Post creation hook for new event trigger. */
    objectaccess_seams::invoke_object_post_create_hook::call(EVENT_TRIGGER_RELATION_ID, trigoid, 0)?;

    /* Close pg_event_trigger. */
    tgrel.close(types_storage::lock::RowExclusiveLock)?;

    Ok(trigoid)
}

/// `filter_list_to_array` (event_trigger.c:359-383) — ASCII-uppercase each tag,
/// to be stored as the catalog `text[]` `evttags` column.
fn filter_list_to_array(filterlist: &[String]) -> Vec<String> {
    filterlist
        .iter()
        .map(|tag| {
            tag.bytes()
                .map(|b| pg_ascii_toupper(b) as char)
                .collect::<String>()
        })
        .collect()
}

/// `pg_ascii_toupper` (utils/adt/ascii.c semantics): uppercase only ASCII a-z.
#[inline]
fn pg_ascii_toupper(ch: u8) -> u8 {
    if ch.is_ascii_lowercase() {
        ch - (b'a' - b'A')
    } else {
        ch
    }
}

/// `SetDatabaseHasLoginEventTriggers` (event_trigger.c:389-421) — set
/// `pg_database.dathasloginevt` for the current database.
pub fn SetDatabaseHasLoginEventTriggers(mcx: Mcx<'_>) -> PgResult<()> {
    backend_catalog_pg_database_seams::set_database_has_login_event_triggers::call(mcx)
}

/// `AlterEventTrigger` (event_trigger.c:426-473) — ALTER EVENT TRIGGER foo
/// ENABLE | DISABLE | ENABLE ALWAYS | REPLICA.
pub fn AlterEventTrigger<'mcx>(mcx: Mcx<'mcx>, trigname: &str, tgenabled: i8) -> PgResult<Oid> {
    let tgrel = backend_access_table_table_seams::table_open::call(
        mcx,
        EVENT_TRIGGER_RELATION_ID,
        types_storage::lock::RowExclusiveLock,
    )?;

    /* tup = SearchSysCacheCopy1(EVENTTRIGGERNAME, ...); evtForm->{oid,evtevent} */
    let (trigoid, evtevent) = match syscache_seams::event_trigger_by_name::call(mcx, trigname)? {
        Some((oid, evtevent, _owner)) => (oid, evtevent.as_str().to_string()),
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("event trigger \"{trigname}\" does not exist"))
                .into_error());
        }
    };

    if !aclchk_seams::object_ownercheck::call(
        EVENT_TRIGGER_RELATION_ID,
        trigoid,
        backend_utils_init_miscinit::GetUserId(),
    )? {
        aclchk_seams::aclcheck_error::call(
            types_acl::ACLCHECK_NOT_OWNER,
            OBJECT_EVENT_TRIGGER,
            Some(trigname.to_string()),
        )?;
    }

    /* tuple is a copy, so we can modify it below: evtForm->evtenabled = tgenabled. */
    let evt_tuple = syscache_seams::search_syscache_copy_pg_event_trigger_tuple::call(mcx, trigoid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for event trigger {trigoid}")))?;
    indexing_seams::catalog_tuple_update_pg_event_trigger_enabled::call(
        mcx,
        &tgrel,
        &evt_tuple,
        tgenabled,
    )?;

    /*
     * Login event triggers have an additional flag in pg_database to enable
     * faster lookups in hot codepaths. Set the flag unless already True.
     */
    if evtevent == "login" && tgenabled != TRIGGER_DISABLED {
        SetDatabaseHasLoginEventTriggers(mcx)?;
    }

    objectaccess_seams::invoke_object_post_alter_hook::call(EVENT_TRIGGER_RELATION_ID, trigoid, 0)?;

    /* clean up */
    tgrel.close(types_storage::lock::RowExclusiveLock)?;

    Ok(trigoid)
}

/// `get_event_trigger_oid(trigname, missing_ok)` (event_trigger.c:578-590) —
/// look up an event trigger by name to find its OID. With `missing_ok = false`
/// a miss raises `ERRCODE_UNDEFINED_OBJECT`; with `missing_ok = true` it returns
/// `InvalidOid`.
pub fn get_event_trigger_oid(trigname: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = syscache_seams::event_trigger_oid_by_name::call(trigname)?.unwrap_or(InvalidOid);
    if oid == InvalidOid && !missing_ok {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("event trigger \"{trigname}\" does not exist"))
            .into_error());
    }
    Ok(oid)
}

/// `AlterEventTriggerOwner_internal` (event_trigger.c:538-571) — the shared
/// owner-change workhorse. `rel` is the open pg_event_trigger relation; the
/// caller has already resolved the trigger's `oid`, current `evtowner`, and
/// `evtname` (for error text) and holds the writable syscache copy `evt_tuple`.
fn alter_event_trigger_owner_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    evt_tuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    oid: Oid,
    evtowner: Oid,
    evtname: &str,
    new_owner_id: Oid,
) -> PgResult<()> {
    /* if (form->evtowner == newOwnerId) return; */
    if evtowner == new_owner_id {
        return Ok(());
    }

    if !aclchk_seams::object_ownercheck::call(
        EVENT_TRIGGER_RELATION_ID,
        oid,
        backend_utils_init_miscinit::GetUserId(),
    )? {
        aclchk_seams::aclcheck_error::call(
            types_acl::ACLCHECK_NOT_OWNER,
            OBJECT_EVENT_TRIGGER,
            Some(evtname.to_string()),
        )?;
    }

    /* New owner must be a superuser */
    if !backend_utils_init_miscinit_seams::superuser_arg::call(new_owner_id)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied to change owner of event trigger \"{evtname}\""
            ))
            .errhint("The owner of an event trigger must be a superuser.")
            .into_error());
    }

    /* form->evtowner = newOwnerId; CatalogTupleUpdate(rel, &tup->t_self, tup); */
    indexing_seams::catalog_tuple_update_pg_event_trigger_owner::call(
        mcx,
        rel,
        evt_tuple,
        new_owner_id,
    )?;

    /* Update owner dependency reference */
    backend_catalog_pg_shdepend::changeDependencyOnOwner(
        EVENT_TRIGGER_RELATION_ID,
        oid,
        new_owner_id,
    )?;

    objectaccess_seams::invoke_object_post_alter_hook::call(EVENT_TRIGGER_RELATION_ID, oid, 0)?;

    Ok(())
}

/// `AlterEventTriggerOwner(const char *name, Oid newOwnerId)`
/// (event_trigger.c:478-507) — ALTER EVENT TRIGGER ... OWNER TO.
pub fn AlterEventTriggerOwner<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    new_owner_id: Oid,
) -> PgResult<ObjectAddress> {
    let rel = backend_access_table_table_seams::table_open::call(
        mcx,
        EVENT_TRIGGER_RELATION_ID,
        types_storage::lock::RowExclusiveLock,
    )?;

    /* tup = SearchSysCacheCopy1(EVENTTRIGGERNAME, ...) */
    let (evt_oid, evtowner) = match syscache_seams::event_trigger_by_name::call(mcx, name)? {
        Some((oid, _evtevent, owner)) => (oid, owner),
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("event trigger \"{name}\" does not exist"))
                .into_error());
        }
    };

    let evt_tuple = syscache_seams::search_syscache_copy_pg_event_trigger_tuple::call(mcx, evt_oid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for event trigger {evt_oid}")))?;

    alter_event_trigger_owner_internal(
        mcx,
        &rel,
        &evt_tuple,
        evt_oid,
        evtowner,
        name,
        new_owner_id,
    )?;

    let address = ObjectAddress {
        classId: EVENT_TRIGGER_RELATION_ID,
        objectId: evt_oid,
        objectSubId: 0,
    };

    rel.close(types_storage::lock::RowExclusiveLock)?;

    Ok(address)
}

/// `AlterEventTriggerOwner_oid(Oid trigOid, Oid newOwnerId)`
/// (event_trigger.c:513-533) — change an event trigger's owner by OID (REASSIGN
/// OWNED / pg_shdepend owner change).
pub fn alter_event_trigger_owner_oid(trig_oid: Oid, new_owner_id: Oid) -> PgResult<()> {
    let scratch = MemoryContext::new("AlterEventTriggerOwner_oid");
    let mcx = scratch.mcx();

    let rel = backend_access_table_table_seams::table_open::call(
        mcx,
        EVENT_TRIGGER_RELATION_ID,
        types_storage::lock::RowExclusiveLock,
    )?;

    /* tup = SearchSysCacheCopy1(EVENTTRIGGEROID, ObjectIdGetDatum(trigOid)) */
    let evt_tuple = syscache_seams::search_syscache_copy_pg_event_trigger_tuple::call(mcx, trig_oid)?
        .ok_or_else(|| {
            ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("event trigger with OID {trig_oid} does not exist"))
                .into_error()
        })?;

    let (evtowner, evtname) =
        syscache_seams::event_trigger_owner_name::call(mcx, trig_oid)?.ok_or_else(|| {
            ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("event trigger with OID {trig_oid} does not exist"))
                .into_error()
        })?;

    alter_event_trigger_owner_internal(
        mcx,
        &rel,
        &evt_tuple,
        trig_oid,
        evtowner,
        evtname.as_str(),
        new_owner_id,
    )?;

    rel.close(types_storage::lock::RowExclusiveLock)?;

    Ok(())
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
    backend_tcop_utility_out_seams::event_trigger_collect_alter_def_privs::set(
        event_trigger_collect_alter_def_privs,
    );
    backend_tcop_utility_out_seams::event_trigger_alter_table_start::set(
        event_trigger_alter_table_start,
    );
    backend_tcop_utility_out_seams::event_trigger_alter_table_relid::set(
        event_trigger_alter_table_relid,
    );
    // tablecmds' `AlterTableInternal` reaches the same C `EventTriggerAlterTableRelid`
    // through its own seam declaration; install the same body there.
    backend_commands_tablecmds_seams::event_trigger_alter_table_relid::set(|object_id| {
        event_trigger_alter_table_relid(object_id);
        Ok(())
    });
    backend_tcop_utility_out_seams::event_trigger_alter_table_end::set(
        event_trigger_alter_table_end,
    );
    backend_tcop_utility_out_seams::event_trigger_supports_object_type::set(
        event_trigger_supports_object_type,
    );
    backend_tcop_utility_out_seams::create_event_trigger::set(create_event_trigger_seam);
    backend_tcop_utility_out_seams::alter_event_trigger::set(alter_event_trigger_seam);

    // The post-gate firing tail (EventTriggerCommonSetup run-list + EventTriggerInvoke).
    fire_seams::event_trigger_fire::set(|parsetree, event, eventstr| {
        event_trigger_fire_impl(parsetree, event, eventstr)
    });

    // PL/pgSQL TG_EVENT / TG_TAG promise accessors — read the currently-firing
    // event trigger's event/tag off the CURRENT_EVENT_TRIGGER side-channel.
    backend_commands_event_trigger_seams::event_trigger_get_event::set(|| {
        Ok(event_trigger_get_event())
    });
    backend_commands_event_trigger_seams::event_trigger_get_tag_name::set(|| {
        Ok(event_trigger_get_tag_name())
    });

    // Inward seams (callers: tcop/utility dispatch via the out-seams above; and
    // catalog/dependency.c's drop-time `sql_drop` collection gate).
    backend_commands_event_trigger_seams::trackDroppedObjectsNeeded::set(
        track_dropped_objects_needed,
    );
    // catalog/dependency.c's drop loop records each dropped object here once a
    // sql_drop / ddl_command_end trigger makes `trackDroppedObjectsNeeded` true.
    backend_commands_event_trigger_seams::EventTriggerSQLDropAddObject::set(
        event_trigger_sql_drop_add_object,
    );
    backend_commands_event_trigger_seams::EventTriggerSupportsObject::set(|object| {
        Ok(event_trigger_supports_object(object))
    });

    // OID-by-name lookup for ALTER / DROP / COMMENT ON / RENAME EVENT TRIGGER by
    // name (objectaddress.c's get_object_address dispatch).
    backend_commands_event_trigger_seams::get_event_trigger_oid::set(get_event_trigger_oid);
    backend_commands_event_trigger_seams::event_trigger_on_login::set(EventTriggerOnLogin);

    // ALTER EVENT TRIGGER ... OWNER TO (by name) + REASSIGN OWNED (by OID).
    backend_commands_event_trigger_seams::AlterEventTriggerOwner::set(|name, new_owner_id| {
        let scratch = MemoryContext::new("AlterEventTriggerOwner");
        AlterEventTriggerOwner(scratch.mcx(), name, new_owner_id)
    });
    backend_commands_event_trigger_seams::alter_event_trigger_owner_oid::set(
        alter_event_trigger_owner_oid,
    );

    // CREATE/ALTER OPERATOR CLASS/FAMILY command collection (opclasscmds.c).
    // No-ops in standalone (no event-trigger state); the active-collection
    // deparse path stops loudly inside each body.
    backend_commands_event_trigger_seams::event_trigger_collect_simple_command::set(
        event_trigger_collect_simple_command_opfamily,
    );
    backend_commands_event_trigger_seams::event_trigger_collect_simple_command_create_schema::set(
        event_trigger_collect_simple_command_create_schema,
    );
    backend_commands_event_trigger_seams::event_trigger_collect_simple_command_reindex::set(
        event_trigger_collect_simple_command_reindex,
    );
    backend_commands_event_trigger_seams::event_trigger_collect_simple_command_publication::set(
        event_trigger_collect_simple_command_publication,
    );
    backend_commands_event_trigger_seams::event_trigger_table_rewrite::set(
        event_trigger_table_rewrite,
    );
    backend_commands_event_trigger_seams::event_trigger_collect_create_opclass::set(
        event_trigger_collect_create_opclass,
    );
    backend_commands_event_trigger_seams::event_trigger_collect_alter_opfam::set(
        event_trigger_collect_alter_opfam,
    );
}

