use cache_evtcache::{
    Anum_pg_event_trigger_evtenabled, Anum_pg_event_trigger_oid, Natts_pg_event_trigger,
    EVENT_TRIGGER_OID_INDEX_ID, EVENT_TRIGGER_RELATION_ID,
};
use datum::Datum;
use mcx::{Mcx, PgVec};
use pg_depend::{DependencyType, ObjectAddress};
use types_core::{CommandTag, InvalidOid, Oid, OidIsValid, NAMEDATALEN, TEXTOID};
use types_error::{
    PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_nodes::parsenodes::{AlterEventTrigStmt, CreateEventTrigStmt};
use types_rel::{AccessExclusiveLock, InplaceUpdateTupleLock, RowExclusiveLock};

use crate::{TRIGGER_DISABLED, TRIGGER_FIRES_ON_ORIGIN};

const PROCEDURE_RELATION_ID: Oid = 1255;
const EVENT_TRIGGEROID: Oid = 3838;

pub fn CreateEventTrigger<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateEventTrigStmt<'mcx>,
) -> PgResult<Oid> {
    let trigname = stmt.trigname.unwrap_or("");
    let eventname = stmt.eventname.unwrap_or("");

    if !superuser::superuser()? {
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied to create event trigger \"{trigname}\""))
            .errhint("Must be superuser to create an event trigger.")
            .into_error()
            .into());
    }

    if !matches!(
        eventname,
        "ddl_command_start" | "ddl_command_end" | "sql_drop" | "login" | "table_rewrite"
    ) {
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("unrecognized event name \"{eventname}\""))
            .into_error()
            .into());
    }

    let mut tags: Option<Vec<&str>> = None;
    for def_node in stmt.whenclause.iter() {
        let def = def_node.as_def_elem().expect("whenclause holds DefElems");
        let defname = def.defname.unwrap_or("");
        if defname == "tag" {
            if tags.is_some() {
                return Err(elog::ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!(
                        "filter variable \"{defname}\" specified more than once"
                    ))
                    .into_error()
                    .into());
            }
            let list = def
                .arg
                .expect("tag filter carries a value list")
                .as_list()
                .expect("tag filter value is a String list");
            let mut vals = Vec::with_capacity(list.len());
            for v in list.iter() {
                vals.push(v.as_string().expect("tag filter values are Strings").sval);
            }
            tags = Some(vals);
        } else {
            return Err(elog::ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("unrecognized filter variable \"{defname}\""))
                .into_error()
                .into());
        }
    }

    if let Some(taglist) = &tags {
        match eventname {
            "ddl_command_start" | "ddl_command_end" | "sql_drop" => {
                validate_ddl_tags("tag", taglist)?
            }
            "table_rewrite" => validate_table_rewrite_tags("tag", taglist)?,
            "login" => {
                return Err(elog::ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("tag filtering is not supported for login event triggers")
                    .into_error()
                    .into())
            }
            _ => {}
        }
    }

    if let Some(tup) = cache_syscache::SearchSysCache1(
        cache_syscache::EVENTTRIGGERNAME,
        cache_syscache::SysCacheKey::Str(trigname),
    )? {
        cache_syscache::ReleaseSysCache(tup);
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("event trigger \"{trigname}\" already exists"))
            .into_error()
            .into());
    }

    let funcoid = LookupFuncName0(mcx, &stmt.funcname)?;
    if lsyscache::function::get_func_rettype(funcoid)? != EVENT_TRIGGEROID {
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "function {} must return type {}",
                name_list_to_string(&stmt.funcname),
                "event_trigger"
            ))
            .into_error()
            .into());
    }

    insert_event_trigger_tuple(
        mcx,
        trigname,
        eventname,
        miscinit::GetUserId(),
        funcoid,
        tags.as_deref(),
    )
}

fn validate_ddl_tags(filtervar: &str, taglist: &[&str]) -> PgResult<()> {
    for tagstr in taglist {
        let tag = cmdtag::GetCommandTagEnum(tagstr.as_bytes());
        if tag == CommandTag::UNKNOWN {
            return Err(elog::ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!(
                    "filter value \"{tagstr}\" not recognized for filter variable \"{filtervar}\""
                ))
                .into_error()
                .into());
        }
        if !cmdtag::command_tag_event_trigger_ok(tag) {
            return Err(elog::ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!("event triggers are not supported for {tagstr}"))
                .into_error()
                .into());
        }
    }
    Ok(())
}

fn validate_table_rewrite_tags(_filtervar: &str, taglist: &[&str]) -> PgResult<()> {
    for tagstr in taglist {
        let tag = cmdtag::GetCommandTagEnum(tagstr.as_bytes());
        if !cmdtag::command_tag_table_rewrite_ok(tag) {
            return Err(elog::ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!("event triggers are not supported for {tagstr}"))
                .into_error()
                .into());
        }
    }
    Ok(())
}

fn LookupFuncName0(mcx: Mcx<'_>, funcname: &types_nodes::list::NodeList<'_>) -> PgResult<Oid> {
    let mut parts: Vec<&str> = Vec::with_capacity(funcname.len());
    for n in funcname.iter() {
        parts.push(n.as_string().expect("funcname holds Strings").sval);
    }
    let candidates = catalog_namespace::FuncnameGetCandidates(mcx, &parts, 0, &[], false, false)?;
    for c in candidates.iter() {
        if c.args.is_empty() && OidIsValid(c.oid) {
            return Ok(c.oid);
        }
    }
    Err(elog::ereport(ERROR)
        .errcode(ERRCODE_UNDEFINED_FUNCTION)
        .errmsg(format!("function {}() does not exist", name_list_to_string_raw(&parts)))
        .into_error()
        .into())
}

fn name_list_to_string(funcname: &types_nodes::list::NodeList<'_>) -> String {
    let parts: Vec<&str> = funcname
        .iter()
        .map(|n| n.as_string().expect("funcname holds Strings").sval)
        .collect();
    name_list_to_string_raw(&parts)
}

fn name_list_to_string_raw(parts: &[&str]) -> String {
    parts.join(".")
}

fn insert_event_trigger_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    trigname: &str,
    eventname: &str,
    evt_owner: Oid,
    funcoid: Oid,
    taglist: Option<&[&str]>,
) -> PgResult<Oid> {
    let tgrel = table::table_open(mcx, EVENT_TRIGGER_RELATION_ID, RowExclusiveLock)?;
    let trigoid = catalog::GetNewOidWithIndex(
        mcx,
        &tgrel,
        EVENT_TRIGGER_OID_INDEX_ID,
        Anum_pg_event_trigger_oid,
    )?;

    let mut values = [Datum::null(); Natts_pg_event_trigger];
    let mut nulls = [false; Natts_pg_event_trigger];
    let evtname = name_arg(mcx, trigname)?;
    let evtevent = name_arg(mcx, eventname)?;
    values[0] = Datum::from_oid(trigoid);
    values[1] = Datum::from_usize(evtname.as_ptr() as usize);
    values[2] = Datum::from_usize(evtevent.as_ptr() as usize);
    values[3] = Datum::from_oid(evt_owner);
    values[4] = Datum::from_oid(funcoid);
    values[5] = Datum::from_i8(TRIGGER_FIRES_ON_ORIGIN);
    let tags_image;
    match taglist {
        None => nulls[6] = true,
        Some(tags) => {
            tags_image = filter_list_to_array(mcx, tags)?;
            values[6] = Datum::from_usize(tags_image.as_ptr() as usize);
        }
    }

    let mut tuple = heaptuple::heap_form_tuple(mcx, tgrel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &tgrel, &mut tuple)?;

    if eventname == "login" {
        SetDatabaseHasLoginEventTriggers(mcx)?;
    }

    pg_depend::recordDependencyOnOwner(mcx, EVENT_TRIGGER_RELATION_ID, trigoid, evt_owner)?;
    let myself = ObjectAddress::set(EVENT_TRIGGER_RELATION_ID, trigoid);
    pg_depend::recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(PROCEDURE_RELATION_ID, funcoid),
        DependencyType::Normal,
    )?;

    tgrel.close(RowExclusiveLock)?;
    Ok(trigoid)
}

fn filter_list_to_array<'mcx>(mcx: Mcx<'mcx>, tags: &[&str]) -> PgResult<PgVec<'mcx, u8>> {
    let mut elems: Vec<Datum> = Vec::with_capacity(tags.len());
    for tag in tags {
        let upper: String =
            tag.chars().map(|c| if c.is_ascii() { c.to_ascii_uppercase() } else { c }).collect();
        let t = varlena::cstring_to_text(mcx, upper.as_bytes())?;
        elems.push(Datum::from_usize(t.into_image().leak().as_ptr() as usize));
    }
    arrayfuncs::construct_md_array(
        mcx,
        &elems,
        None,
        1,
        &[tags.len() as i32],
        &[1],
        TEXTOID,
        -1,
        false,
        b'i',
    )
}

fn name_arg<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgVec<'mcx, u8>> {
    let n = NAMEDATALEN as usize;
    assert!(name.len() < n, "event trigger name overflows NAMEDATALEN: {name:?}");
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..n - name.len()])?;
    Ok(buf)
}

pub fn AlterEventTrigger<'mcx>(mcx: Mcx<'mcx>, stmt: &AlterEventTrigStmt<'mcx>) -> PgResult<Oid> {
    let trigname = stmt.trigname.unwrap_or("");
    let tgenabled = stmt.tgenabled;

    let tgrel = table::table_open(mcx, EVENT_TRIGGER_RELATION_ID, RowExclusiveLock)?;
    let Some(tup) = cache_syscache::SearchSysCacheCopy(
        mcx,
        cache_syscache::EVENTTRIGGERNAME,
        cache_syscache::SysCacheKey::Str(trigname),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?
    else {
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("event trigger \"{trigname}\" does not exist"))
            .into_error()
            .into());
    };

    let descr = tgrel.descr();
    let mut isnull = false;
    // SAFETY (each): fixed NOT NULL pg_event_trigger columns (pg_event_trigger.h).
    let trigoid = unsafe {
        types_tuple::heap_getattr(&tup, Anum_pg_event_trigger_oid as i32, descr, &mut isnull)
    }
    .as_oid();
    let evtevent_ptr = unsafe {
        types_tuple::heap_getattr(
            &tup,
            cache_evtcache::Anum_pg_event_trigger_evtevent as i32,
            descr,
            &mut isnull,
        )
    }
    .as_usize() as *const u8;
    let is_login = unsafe {
        core::slice::from_raw_parts(evtevent_ptr, NAMEDATALEN as usize).starts_with(b"login\0")
    };

    // object_ownercheck: only superusers can own event triggers.
    if !superuser::superuser_arg(miscinit::GetUserId())? {
        panic!("AlterEventTrigger: object_ownercheck for non-superusers (acl lane)");
    }

    let natts = descr.natts as usize;
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[Anum_pg_event_trigger_evtenabled as usize - 1] = Datum::from_i8(tgenabled);
    repl[Anum_pg_event_trigger_evtenabled as usize - 1] = true;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, &tup, descr, &repl_values, &repl_isnull, &repl)?;
    let otid = tup.t_self;
    catalog_indexing::CatalogTupleUpdate(mcx, &tgrel, &otid, &mut newtup)?;

    if is_login && tgenabled != TRIGGER_DISABLED {
        SetDatabaseHasLoginEventTriggers(mcx)?;
    }

    tgrel.close(RowExclusiveLock)?;
    Ok(trigoid)
}

// AlterEventTriggerOwner + AlterEventTriggerOwner_internal (event_trigger.c).
pub fn AlterEventTriggerOwner<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    newOwnerId: Oid,
) -> PgResult<ObjectAddress> {
    let rel = table::table_open(mcx, EVENT_TRIGGER_RELATION_ID, RowExclusiveLock)?;
    let Some(tup) = cache_syscache::SearchSysCacheCopy(
        mcx,
        cache_syscache::EVENTTRIGGERNAME,
        cache_syscache::SysCacheKey::Str(name),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?
    else {
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("event trigger \"{name}\" does not exist"))
            .into_error()
            .into());
    };

    let descr = rel.descr();
    let mut isnull = false;
    // SAFETY (each): fixed NOT NULL pg_event_trigger columns (pg_event_trigger.h).
    let evt_oid = unsafe {
        types_tuple::heap_getattr(&tup, Anum_pg_event_trigger_oid as i32, descr, &mut isnull)
    }
    .as_oid();
    let evtowner = unsafe {
        types_tuple::heap_getattr(
            &tup,
            cache_evtcache::Anum_pg_event_trigger_evtowner as i32,
            descr,
            &mut isnull,
        )
    }
    .as_oid();

    if evtowner != newOwnerId {
        // object_ownercheck: event-trigger owners are always superusers, and
        // superusers pass every ownercheck; a non-superuser caller cannot own one.
        if !superuser::superuser_arg(miscinit::GetUserId())? {
            panic!("AlterEventTriggerOwner: object_ownercheck for non-superusers (acl lane)");
        }
        if !superuser::superuser_arg(newOwnerId)? {
            return Err(elog::ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!(
                    "permission denied to change owner of event trigger \"{name}\""
                ))
                .errhint("The owner of an event trigger must be a superuser.")
                .into_error()
                .into());
        }

        let natts = descr.natts as usize;
        let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        repl_values.resize(natts, Datum::null());
        repl_isnull.resize(natts, false);
        repl.resize(natts, false);
        repl_values[cache_evtcache::Anum_pg_event_trigger_evtowner as usize - 1] =
            Datum::from_oid(newOwnerId);
        repl[cache_evtcache::Anum_pg_event_trigger_evtowner as usize - 1] = true;
        let mut newtup =
            heaptuple::heap_modify_tuple(mcx, &tup, descr, &repl_values, &repl_isnull, &repl)?;
        let otid = tup.t_self;
        catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;

        pg_shdepend::changeDependencyOnOwner(
            mcx,
            EVENT_TRIGGER_RELATION_ID,
            evt_oid,
            newOwnerId,
        )?;
    }

    rel.close(RowExclusiveLock)?;
    Ok(ObjectAddress::set(EVENT_TRIGGER_RELATION_ID, evt_oid))
}


// AlterEventTriggerOwner_oid + _internal (event_trigger.c).
pub fn AlterEventTriggerOwner_oid<'mcx>(
    mcx: Mcx<'mcx>,
    trig_oid: Oid,
    new_owner_id: Oid,
) -> PgResult<()> {
    let rel = table::table_open(mcx, EVENT_TRIGGER_RELATION_ID, RowExclusiveLock)?;

    let Some(tup) = cache_syscache::SearchSysCacheCopy(
        mcx,
        cache_syscache::EVENTTRIGGEROID,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(trig_oid)),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?
    else {
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("event trigger with OID {trig_oid} does not exist"))
            .into_error()
            .into());
    };

    AlterEventTriggerOwner_internal(mcx, &rel, &tup, new_owner_id)?;

    rel.close(RowExclusiveLock)
}


fn AlterEventTriggerOwner_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    tup: &heaptuple::HeapTuple<'mcx>,
    new_owner_id: Oid,
) -> PgResult<()> {
    let descr = rel.descr();
    let mut isnull = false;
    // SAFETY (each): fixed NOT NULL pg_event_trigger columns (pg_event_trigger.h).
    let trigoid = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_event_trigger_oid as i32, descr, &mut isnull)
    }
    .as_oid();
    let evtowner = unsafe {
        types_tuple::heap_getattr(
            tup,
            cache_evtcache::Anum_pg_event_trigger_evtowner as i32,
            descr,
            &mut isnull,
        )
    }
    .as_oid();
    let name_ptr = unsafe {
        types_tuple::heap_getattr(
            tup,
            cache_evtcache::Anum_pg_event_trigger_evtname as i32,
            descr,
            &mut isnull,
        )
    }
    .as_usize() as *const types_tuple::NameData;
    // SAFETY: inline NAMEDATALEN name column of the copied tuple.
    let name = unsafe { core::str::from_utf8_unchecked((*name_ptr).name_str()) };

    if evtowner == new_owner_id {
        return Ok(());
    }

    // object_ownercheck: only superusers can own event triggers.
    if !superuser::superuser_arg(miscinit::GetUserId())? {
        panic!("AlterEventTriggerOwner_internal: object_ownercheck for non-superusers (acl lane)");
    }

    if !superuser::superuser_arg(new_owner_id)? {
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied to change owner of event trigger \"{name}\""
            ))
            .errhint("The owner of an event trigger must be a superuser.")
            .into_error()
            .into());
    }

    let natts = descr.natts as usize;
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[cache_evtcache::Anum_pg_event_trigger_evtowner as usize - 1] =
        Datum::from_oid(new_owner_id);
    repl[cache_evtcache::Anum_pg_event_trigger_evtowner as usize - 1] = true;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, tup, descr, &repl_values, &repl_isnull, &repl)?;
    let otid = tup.t_self;
    catalog_indexing::CatalogTupleUpdate(mcx, rel, &otid, &mut newtup)?;

    pg_shdepend::changeDependencyOnOwner(mcx, EVENT_TRIGGER_RELATION_ID, trigoid, new_owner_id)
}


// SetDatabaseHasLoginEventTriggers (event_trigger.c): the shared-object lock
// is a custom tag serializing this against EventTriggerOnLogin's flag reset;
// SearchSysCacheLockedCopy1 is composed from Locked1 + copytuple here.
pub(crate) fn SetDatabaseHasLoginEventTriggers(mcx: Mcx<'_>) -> PgResult<()> {
    let dbid = init_small::globals::MyDatabaseId();
    let pg_db = table::table_open(mcx, types_core::catalog::DATABASE_RELATION_ID, RowExclusiveLock)?;
    lmgr::LockSharedObject(types_core::catalog::DATABASE_RELATION_ID, dbid, 0, AccessExclusiveLock)?;
    let Some(ctup) = cache_syscache::SearchSysCacheLocked1(
        cache_syscache::cacheinfo::DATABASEOID,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(dbid)),
    )?
    else {
        return Err(elog::ereport(ERROR)
            .errmsg(format!("cache lookup failed for database {dbid}"))
            .into_error()
            .into());
    };
    let tuple = heaptuple::heap_copytuple(mcx, &ctup.tuple())?;
    cache_syscache::ReleaseSysCache(ctup);
    let otid = tuple.as_tuple().t_self;
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
    if !hasloginevt {
        let natts = descr.natts as usize;
        let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        repl_values.resize(natts, Datum::null());
        repl_isnull.resize(natts, false);
        repl.resize(natts, false);
        repl_values[pg_database::Anum_pg_database_dathasloginevt as usize - 1] =
            Datum::from_bool(true);
        repl[pg_database::Anum_pg_database_dathasloginevt as usize - 1] = true;
        let mut newtup = heaptuple::heap_modify_tuple(
            mcx,
            tuple.as_tuple(),
            descr,
            &repl_values,
            &repl_isnull,
            &repl,
        )?;
        catalog_indexing::CatalogTupleUpdate(mcx, &pg_db, &otid, &mut newtup)?;
        xact::CommandCounterIncrement()?;
    }
    lmgr::UnlockTuple(&pg_db, &otid, InplaceUpdateTupleLock)?;
    pg_db.close(RowExclusiveLock)?;
    Ok(())
}

pub fn get_event_trigger_oid(trigname: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = cache_syscache::GetSysCacheOid(
        cache_syscache::EVENTTRIGGERNAME,
        Anum_pg_event_trigger_oid as i32,
        cache_syscache::SysCacheKey::Str(trigname),
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
        cache_syscache::SysCacheKey::UNUSED,
    )?;
    if !OidIsValid(oid) && !missing_ok {
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("event trigger \"{trigname}\" does not exist"))
            .into_error()
            .into());
    }
    Ok(oid)
}
