#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::cell::{Cell, RefCell};

use cache_evtcache::{EventCacheLookup, EventTriggerEvent};
use mcx::Mcx;
use pg_depend::ObjectAddress;
use types_core::{CommandTag, InvalidOid, Oid, OidIsValid};
use types_error::PgResult;

mod ddl;
mod sqldrop;
mod srf;

pub use cache_evtcache::EVENT_TRIGGER_RELATION_ID;
pub use ddl::{
    get_event_trigger_oid, AlterEventTrigger, AlterEventTriggerOwner,
    AlterEventTriggerOwner_oid, CreateEventTrigger,
};
pub use sqldrop::EventTriggerSQLDropAddObject;

const TRIGGER_FIRES_ON_ORIGIN: i8 = b'O' as i8;
const TRIGGER_FIRES_ON_REPLICA: i8 = b'R' as i8;
pub(crate) const TRIGGER_DISABLED: i8 = b'D' as i8;

const SESSION_REPLICATION_ROLE_ORIGIN: i32 = 0;
const SESSION_REPLICATION_ROLE_REPLICA: i32 = 1;

// fcinfo->context node for the firing call (CALLED_AS_EVENT_TRIGGER demux).
#[repr(C)]
pub struct EventTriggerData {
    pub node: fmgr::FmNode,
    pub event: &'static str,
    pub tag: CommandTag,
}

pub const T_EVENT_TRIGGER_DATA: u32 = types_nodes::NodeTag::T_EventTriggerData as u32;

pub(crate) struct SQLDropObject {
    pub address: ObjectAddress,
    pub schemaname: Option<String>,
    pub objname: Option<String>,
    pub objidentity: Option<String>,
    pub objecttype: Option<String>,
    pub addrnames: Option<Vec<String>>,
    pub addrargs: Option<Vec<String>>,
    pub original: bool,
    pub normal: bool,
    pub istemp: bool,
}

pub(crate) enum CollectedCommandData {
    Simple,
    AlterTable { class_id: Oid, object_id: Oid, nsubcmds: usize },
    // C copies the whole InternalGrant; the ported SRF surface reads only
    // is_grant (via tag) and objtype.
    Grant { objtype: types_nodes::parsenodes::ObjectType },
    // C copies the AlterDefaultPrivilegesStmt; only objtype is SRF-visible.
    DefPrivs { objtype: types_nodes::parsenodes::ObjectType },
}

// C stores copyObject(parsetree) and derives the tag lazily in the SRF; node
// deep-copy has no port, so the tag is captured at collect time (same value).
pub(crate) struct CollectedCommand {
    pub in_extension: bool,
    pub tag: CommandTag,
    pub address: ObjectAddress,
    pub secondary_object: ObjectAddress,
    pub data: CollectedCommandData,
}

pub(crate) struct EventTriggerQueryState {
    pub sql_drop_list: Vec<SQLDropObject>,
    pub in_sql_drop: bool,
    pub table_rewrite_oid: Oid,
    pub table_rewrite_reason: i32,
    pub command_collection_inhibited: bool,
    pub command_list: Vec<CollectedCommand>,
    // C's currentCommand->parent chain as a stack; top = currentCommand.
    pub current_command: Vec<CollectedCommand>,
}

thread_local! {
    // currentEventTriggerState; last element is current, rest are `previous`.
    pub(crate) static CURRENT_STATE: RefCell<Vec<EventTriggerQueryState>> =
        const { RefCell::new(Vec::new()) };
    static EVENT_TRIGGERS_GUC: Cell<bool> = const { Cell::new(true) };
    static SESSION_REPLICATION_ROLE: Cell<i32> =
        const { Cell::new(SESSION_REPLICATION_ROLE_ORIGIN) };
}

pub(crate) fn state_is_set() -> bool {
    CURRENT_STATE.with(|s| !s.borrow().is_empty())
}

pub fn trackDroppedObjectsNeeded(mcx: Mcx<'_>) -> PgResult<bool> {
    Ok(!EventCacheLookup(mcx, EventTriggerEvent::SqlDrop)?.is_empty()
        || !EventCacheLookup(mcx, EventTriggerEvent::TableRewrite)?.is_empty()
        || !EventCacheLookup(mcx, EventTriggerEvent::DdlCommandEnd)?.is_empty())
}

pub fn EventTriggerBeginCompleteQuery(mcx: Mcx<'_>) -> PgResult<bool> {
    if !trackDroppedObjectsNeeded(mcx)? {
        return Ok(false);
    }
    CURRENT_STATE.with(|s| {
        let mut stack = s.borrow_mut();
        let inhibited =
            stack.last().map(|st| st.command_collection_inhibited).unwrap_or(false);
        stack.push(EventTriggerQueryState {
            sql_drop_list: Vec::new(),
            in_sql_drop: false,
            table_rewrite_oid: InvalidOid,
            table_rewrite_reason: 0,
            command_collection_inhibited: inhibited,
            command_list: Vec::new(),
            current_command: Vec::new(),
        });
    });
    Ok(true)
}

pub fn EventTriggerEndCompleteQuery() {
    CURRENT_STATE.with(|s| {
        s.borrow_mut().pop();
    });
}

pub fn EventTriggerInhibitCommandCollection() {
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            st.command_collection_inhibited = true;
        }
    });
}

pub fn EventTriggerUndoInhibitCommandCollection() {
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            st.command_collection_inhibited = false;
        }
    });
}

fn event_triggers_active() -> bool {
    init_small::globals::IsUnderPostmaster() && EVENT_TRIGGERS_GUC.with(|c| c.get())
}

fn filter_event_trigger(tag: CommandTag, item: &cache_evtcache::EventTriggerCacheItem) -> bool {
    let replica =
        SESSION_REPLICATION_ROLE.with(|c| c.get()) == SESSION_REPLICATION_ROLE_REPLICA;
    if replica {
        if item.enabled == TRIGGER_FIRES_ON_ORIGIN {
            return false;
        }
    } else if item.enabled == TRIGGER_FIRES_ON_REPLICA {
        return false;
    }
    if let Some(tagset) = &item.tagset {
        if !tagset.is_member(tag.0) {
            return false;
        }
    }
    true
}

// EventTriggerCommonSetup; the tag is threaded from the caller instead of
// re-derived from the parse tree (CreateCommandTag lives above this crate).
fn event_trigger_common_setup(
    mcx: Mcx<'_>,
    tag: CommandTag,
    event: EventTriggerEvent,
    unfiltered: bool,
) -> PgResult<Vec<Oid>> {
    debug_assert!(match event {
        EventTriggerEvent::TableRewrite => cmdtag::command_tag_table_rewrite_ok(tag),
        _ => cmdtag::command_tag_event_trigger_ok(tag),
    });
    let cachelist = EventCacheLookup(mcx, event)?;
    let mut runlist = Vec::new();
    for item in cachelist.iter() {
        if unfiltered || filter_event_trigger(tag, item) {
            runlist.push(item.fnoid);
        }
    }
    Ok(runlist)
}

fn EventTriggerInvoke(fn_oid_list: &[Oid], event: &'static str, tag: CommandTag) -> PgResult<()> {
    // C: check_stack_depth() — recursion guard unported repo-wide (stack lane).
    let mut first = true;
    for &fnoid in fn_oid_list {
        if first {
            first = false;
        } else {
            xact::CommandCounterIncrement()?;
        }
        let mut trigdata = EventTriggerData {
            node: fmgr::FmNode { tag: T_EVENT_TRIGGER_DATA },
            event,
            tag,
        };
        let mut flinfo = fmgr_core::fmgr_info(fnoid)?;
        let mut fcinfo = fmgr::LocalFcinfo::<0>::new(InvalidOid);
        fcinfo.context = Some(core::ptr::NonNull::from(&mut trigdata).cast());
        // C: pgstat_init_function_usage's `pgstat_track_functions <= fn_stats`
        // early-out, hoisted to the caller as the crate's API requires.
        let fcu = if flinfo.fn_stats < fmgr::TRACK_FUNC_ALL
            && pgstat::function::pgstat_track_functions() > flinfo.fn_stats as i32
        {
            Some(pgstat::function::pgstat_init_function_usage(flinfo.fn_oid)?)
        } else {
            None
        };
        flinfo.invoke(&mut fcinfo)?;
        if let Some(fcu) = &fcu {
            pgstat::function::pgstat_end_function_usage(fcu, true);
        }
    }
    Ok(())
}

pub fn EventTriggerDDLCommandStart(mcx: Mcx<'_>, tag: CommandTag) -> PgResult<()> {
    if !event_triggers_active() {
        return Ok(());
    }
    let runlist = event_trigger_common_setup(mcx, tag, EventTriggerEvent::DdlCommandStart, false)?;
    if runlist.is_empty() {
        return Ok(());
    }
    EventTriggerInvoke(&runlist, "ddl_command_start", tag)?;
    xact::CommandCounterIncrement()?;
    Ok(())
}

pub fn EventTriggerDDLCommandEnd(mcx: Mcx<'_>, tag: CommandTag) -> PgResult<()> {
    if !event_triggers_active() {
        return Ok(());
    }
    // No state means no relevant triggers existed at command start; firing a
    // newer trigger here would break pg_event_trigger_ddl_commands.
    if !state_is_set() {
        return Ok(());
    }
    let runlist = event_trigger_common_setup(mcx, tag, EventTriggerEvent::DdlCommandEnd, false)?;
    if runlist.is_empty() {
        return Ok(());
    }
    xact::CommandCounterIncrement()?;
    EventTriggerInvoke(&runlist, "ddl_command_end", tag)
}

pub fn EventTriggerSQLDrop(mcx: Mcx<'_>, tag: CommandTag) -> PgResult<()> {
    if !event_triggers_active() {
        return Ok(());
    }
    let have_drops = CURRENT_STATE
        .with(|s| s.borrow().last().map(|st| !st.sql_drop_list.is_empty()).unwrap_or(false));
    if !have_drops {
        return Ok(());
    }
    let runlist = event_trigger_common_setup(mcx, tag, EventTriggerEvent::SqlDrop, false)?;
    if runlist.is_empty() {
        return Ok(());
    }
    xact::CommandCounterIncrement()?;
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            st.in_sql_drop = true;
        }
    });
    let res = EventTriggerInvoke(&runlist, "sql_drop", tag);
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            st.in_sql_drop = false;
        }
    });
    res
}

// EventTriggerOnLogin (event_trigger.c): fires once during backend startup;
// when no login trigger remains it opportunistically clears the stale
// pg_database.dathasloginevt fast flag under a conditional custom lock.
pub fn EventTriggerOnLogin(mcx: Mcx<'_>) -> PgResult<()> {
    if !event_triggers_active()
        || !OidIsValid(init_small::globals::MyDatabaseId())
        || !init_small::globals::MyDatabaseHasLoginEventTriggers()
    {
        return Ok(());
    }
    let tag = cmdtag::GetCommandTagEnum(b"LOGIN");
    xact::StartTransactionCommand()?;
    let runlist = event_trigger_common_setup(mcx, tag, EventTriggerEvent::Login, false)?;
    if !runlist.is_empty() {
        let snapshot = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snapshot)?;
        EventTriggerInvoke(&runlist, "login", tag)?;
        snapmgr::PopActiveSnapshot()?;
    } else if lmgr::ConditionalLockSharedObject(
        types_core::catalog::DATABASE_RELATION_ID,
        init_small::globals::MyDatabaseId(),
        0,
        types_rel::AccessExclusiveLock,
    )? {
        // Under the lock a concurrent CREATE/ALTER sets the flag only after
        // inserting the trigger, so an empty unfiltered list is conclusive.
        let runlist = event_trigger_common_setup(mcx, tag, EventTriggerEvent::Login, true)?;
        if runlist.is_empty() {
            reset_database_has_login_event_triggers(mcx)?;
        }
    }
    xact::CommitTransactionCommand()?;
    Ok(())
}

// C: inplace update rather than a regular one — no row-lock wait, no TOAST.
fn reset_database_has_login_event_triggers(mcx: Mcx<'_>) -> PgResult<()> {
    let dbid = init_small::globals::MyDatabaseId();
    let pg_db =
        table::table_open(mcx, types_core::catalog::DATABASE_RELATION_ID, types_rel::RowExclusiveLock)?;
    let mut key = types_scan::ScanKeyData::empty();
    key.sk_attno = pg_database::Anum_pg_database_oid as types_core::AttrNumber;
    key.sk_strategy = types_scan::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)?;
    key.sk_argument = datum::Datum::from_oid(dbid);
    let Some((tuple, state)) = genam::systable_inplace_update_begin(
        mcx,
        &pg_db,
        pg_database::DatabaseOidIndexId,
        true,
        &[key],
    )?
    else {
        return Err(elog::ereport(types_error::ERROR)
            .errmsg(format!("could not find tuple for database {dbid}"))
            .into_error()
            .into());
    };
    let descr = pg_db.descr();
    let mut isnull = false;
    // SAFETY: dathasloginevt is a fixed NOT NULL pg_database column.
    let hasloginevt = unsafe {
        types_tuple::heap_getattr(
            tuple.as_tuple(),
            pg_database::Anum_pg_database_dathasloginevt,
            descr,
            &mut isnull,
        )
    }
    .as_bool();
    if hasloginevt {
        let natts = descr.natts as usize;
        let mut values = vec![datum::Datum::null(); natts];
        let mut nulls = vec![false; natts];
        let mut replace = vec![false; natts];
        values[pg_database::Anum_pg_database_dathasloginevt as usize - 1] =
            datum::Datum::from_bool(false);
        replace[pg_database::Anum_pg_database_dathasloginevt as usize - 1] = true;
        let newtup =
            heaptuple::heap_modify_tuple(mcx, tuple.as_tuple(), descr, &values, &nulls, &replace)?;
        genam::systable_inplace_update_finish(mcx, state, newtup.as_tuple())?;
    } else {
        genam::systable_inplace_update_cancel(mcx, state)?;
    }
    pg_db.close(types_rel::RowExclusiveLock)?;
    Ok(())
}

pub fn EventTriggerTableRewrite(
    mcx: Mcx<'_>,
    tag: CommandTag,
    table_oid: Oid,
    reason: i32,
) -> PgResult<()> {
    if !event_triggers_active() {
        return Ok(());
    }
    if !state_is_set() {
        return Ok(());
    }
    let runlist = event_trigger_common_setup(mcx, tag, EventTriggerEvent::TableRewrite, false)?;
    if runlist.is_empty() {
        return Ok(());
    }
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            st.table_rewrite_oid = table_oid;
            st.table_rewrite_reason = reason;
        }
    });
    let res = EventTriggerInvoke(&runlist, "table_rewrite", tag);
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            st.table_rewrite_oid = InvalidOid;
            st.table_rewrite_reason = 0;
        }
    });
    res?;
    xact::CommandCounterIncrement()?;
    Ok(())
}

pub fn EventTriggerSupportsObjectType(obtype: types_nodes::parsenodes::ObjectType) -> bool {
    use types_nodes::parsenodes::ObjectType::*;
    !matches!(
        obtype,
        OBJECT_DATABASE | OBJECT_TABLESPACE | OBJECT_ROLE | OBJECT_PARAMETER_ACL
            | OBJECT_EVENT_TRIGGER
    )
}

const DATABASE_RELATION_ID: Oid = 1262;
const TABLE_SPACE_RELATION_ID: Oid = 1213;
const AUTH_ID_RELATION_ID: Oid = 1260;
const AUTH_MEM_RELATION_ID: Oid = 1261;
const PARAMETER_ACL_RELATION_ID: Oid = 6243;

pub fn EventTriggerSupportsObject(object: &ObjectAddress) -> bool {
    !matches!(
        object.classId,
        DATABASE_RELATION_ID
            | TABLE_SPACE_RELATION_ID
            | AUTH_ID_RELATION_ID
            | AUTH_MEM_RELATION_ID
            | PARAMETER_ACL_RELATION_ID
            | EVENT_TRIGGER_RELATION_ID
    )
}

pub fn EventTriggerCollectionActive() -> bool {
    collecting()
}

pub(crate) fn collecting() -> bool {
    CURRENT_STATE
        .with(|s| s.borrow().last().map(|st| !st.command_collection_inhibited).unwrap_or(false))
}

// creating_extension (extension.c) — extensions unported; never in-extension.
pub(crate) fn creating_extension() -> bool {
    false
}

pub fn EventTriggerCollectSimpleCommand(
    address: ObjectAddress,
    secondary_object: ObjectAddress,
    tag: CommandTag,
) {
    if !collecting() {
        return;
    }
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            st.command_list.push(CollectedCommand {
                in_extension: creating_extension(),
                tag,
                address,
                secondary_object,
                data: CollectedCommandData::Simple,
            });
        }
    });
}

// EventTriggerCollectGrant (event_trigger.c), with the ExecGrantStmt_oids
// call-site guard EventTriggerSupportsObjectType (aclchk.c:654) folded in.
pub fn EventTriggerCollectGrant(is_grant: bool, objtype: types_nodes::parsenodes::ObjectType) {
    if !EventTriggerSupportsObjectType(objtype) || !collecting() {
        return;
    }
    let tag = cmdtag::GetCommandTagEnum(if is_grant { b"GRANT" } else { b"REVOKE" });
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            st.command_list.push(CollectedCommand {
                in_extension: creating_extension(),
                tag,
                address: ObjectAddress::set(InvalidOid, InvalidOid),
                secondary_object: ObjectAddress::set(InvalidOid, InvalidOid),
                data: CollectedCommandData::Grant { objtype },
            });
        }
    });
}

// EventTriggerCollectAlterDefPrivs (event_trigger.c:2023).
pub fn EventTriggerCollectAlterDefPrivs(
    tag: CommandTag,
    objtype: types_nodes::parsenodes::ObjectType,
) {
    if !collecting() {
        return;
    }
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            st.command_list.push(CollectedCommand {
                in_extension: creating_extension(),
                tag,
                address: ObjectAddress::set(InvalidOid, InvalidOid),
                secondary_object: ObjectAddress::set(InvalidOid, InvalidOid),
                data: CollectedCommandData::DefPrivs { objtype },
            });
        }
    });
}

pub fn EventTriggerAlterTableStart(tag: CommandTag) {
    if !collecting() {
        return;
    }
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            st.current_command.push(CollectedCommand {
                in_extension: creating_extension(),
                tag,
                address: ObjectAddress::set(InvalidOid, InvalidOid),
                secondary_object: ObjectAddress::set(InvalidOid, InvalidOid),
                data: CollectedCommandData::AlterTable {
                    class_id: types_core::RELATION_RELATION_ID,
                    object_id: InvalidOid,
                    nsubcmds: 0,
                },
            });
        }
    });
}

pub fn EventTriggerAlterTableRelid(object_id: Oid) {
    if !collecting() {
        return;
    }
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            let cur = st
                .current_command
                .last_mut()
                .expect("EventTriggerAlterTableRelid without AlterTableStart");
            match &mut cur.data {
                CollectedCommandData::AlterTable { object_id: oid, .. } => *oid = object_id,
                _ => panic!("EventTriggerAlterTableRelid: currentCommand is not AlterTable"),
            }
        }
    });
}

// C stores each subcommand's copyObject + address for extension deparse; only
// the count is observable through the ported surface (whether the enclosing
// ALTER TABLE row is collected at all).
pub fn EventTriggerCollectAlterTableSubcmd(_address: ObjectAddress) {
    if !collecting() {
        return;
    }
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            let cur = st
                .current_command
                .last_mut()
                .expect("EventTriggerCollectAlterTableSubcmd without AlterTableStart");
            match &mut cur.data {
                CollectedCommandData::AlterTable { nsubcmds, object_id, .. } => {
                    debug_assert!(OidIsValid(*object_id));
                    *nsubcmds += 1;
                }
                _ => {
                    panic!("EventTriggerCollectAlterTableSubcmd: currentCommand is not AlterTable")
                }
            }
        }
    });
}

// ProcessUtilityForAlterTable's close half (utility.c:1959-1989): append the
// in-progress ALTER TABLE package before a sub-statement collects, so command
// order matches execution order; the caller reopens with Start+Relid.
pub fn EventTriggerAlterTableSuspend() -> Option<(CommandTag, Oid)> {
    if !collecting() {
        return None;
    }
    let saved = CURRENT_STATE.with(|s| {
        s.borrow().last().and_then(|st| {
            st.current_command.last().and_then(|cur| match cur.data {
                CollectedCommandData::AlterTable { object_id, .. } => Some((cur.tag, object_id)),
                _ => None,
            })
        })
    });
    if saved.is_some() {
        EventTriggerAlterTableEnd();
    }
    saved
}

pub fn EventTriggerAlterTableEnd() {
    if !collecting() {
        return;
    }
    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            let cur = st
                .current_command
                .pop()
                .expect("EventTriggerAlterTableEnd without AlterTableStart");
            let keep =
                matches!(cur.data, CollectedCommandData::AlterTable { nsubcmds, .. } if nsubcmds > 0);
            if keep {
                st.command_list.push(cur);
            }
        }
    });
}

pub fn init_seams() {
    event_trigger_seams::track_dropped_objects_needed::set(trackDroppedObjectsNeeded);
    pg_shdepend::alter_event_trigger_owner_oid::set(ddl::AlterEventTriggerOwner_oid);
    event_trigger_seams::event_trigger_supports_object::set(EventTriggerSupportsObject);
    event_trigger_seams::event_trigger_sql_drop_add_object::set(
        sqldrop::EventTriggerSQLDropAddObject,
    );
    event_trigger_seams::event_trigger_collect_grant::set(EventTriggerCollectGrant);
    event_trigger_seams::event_trigger_on_login::set(EventTriggerOnLogin);
    fmgr_core::register_late_builtins(srf::EVENT_TRIGGER_BUILTINS);

    guc_tables::vars::event_triggers.install(guc_tables::GucVarAccessors {
        get: || EVENT_TRIGGERS_GUC.with(|c| c.get()),
        set: |v| EVENT_TRIGGERS_GUC.with(|c| c.set(v)),
    });
    // session_replication_role's C owner is guc backing storage + an assign
    // hook that resets the plan cache; hosted here until a guc-hooks lane
    // claims it.
    guc_tables::vars::SessionReplicationRole.install(guc_tables::GucVarAccessors {
        get: || SESSION_REPLICATION_ROLE.with(|c| c.get()),
        set: |v| SESSION_REPLICATION_ROLE.with(|c| c.set(v)),
    });
    guc_tables::hooks::assign_session_replication_role.install(|newval, _extra| {
        if SESSION_REPLICATION_ROLE.with(|c| c.get()) != newval {
            plancache::ResetPlanCache();
        }
    });
}
