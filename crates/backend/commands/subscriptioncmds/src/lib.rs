// subscriptioncmds.c: subscription DDL. Logical-replication execution
// (walreceiver connections, tablesync, replication slots) is unported; every
// path that would touch a publisher is a loud panic, except libpq's
// networking-free conninfo/port validation (see conninfo.rs).
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod connect;
mod conninfo;
mod origin;


use datum::Datum;
use mcx::{Mcx, PgString, PgVec};
use types_core::catalog::DATABASE_RELATION_ID;
use types_core::fmgr::NAMEDATALEN;
use types_core::primitive::XLogRecPtr;
use types_core::{AttrNumber, InvalidOid, InvalidXLogRecPtr, Oid, TEXTOID};
use types_error::{
    ErrorLocation, PgError, PgResult, SqlState, ERRCODE_CONNECTION_FAILURE,
    ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_NAME,
    ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NAME_TOO_LONG,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT,
    ErrorLevel, NOTICE, WARNING,
};
use types_guc::{PGC_BACKEND, PGC_S_TEST};
use types_nodes::parsenodes::{
    AlterSubscriptionStmt, AlterSubscriptionType, CreateSubscriptionStmt, DefElem,
    DropSubscriptionStmt, ObjectType,
};
use types_nodes::NodeList;
use types_rel::{AccessExclusiveLock, NoLock, Relation, RowExclusiveLock};
use types_tuple::{HeapTupleData, NameData, TupleDescData};

use aclchk::{ACLCHECK_NOT_OWNER, ACLCHECK_OK};
use adt_acl::ACL_CREATE;
use cache_syscache::cacheinfo::{SUBSCRIPTIONNAME, SUBSCRIPTIONOID};
use cache_syscache::{GetSysCacheOid, SearchSysCacheCopy, SysCacheKey};
use guc::GUC_ACTION_SET;
use pg_depend::ObjectAddress;
use pg_subscription::{
    Anum_pg_subscription_oid, Anum_pg_subscription_subbinary, Anum_pg_subscription_subconninfo,
    Anum_pg_subscription_subdbid, Anum_pg_subscription_subdisableonerr,
    Anum_pg_subscription_subenabled, Anum_pg_subscription_subfailover,
    Anum_pg_subscription_subname, Anum_pg_subscription_subowner,
    Anum_pg_subscription_subpasswordrequired, Anum_pg_subscription_subpublications,
    Anum_pg_subscription_subrunasowner, Anum_pg_subscription_subskiplsn,
    Anum_pg_subscription_subslotname, Anum_pg_subscription_substream,
    Anum_pg_subscription_subsynccommit, Anum_pg_subscription_subtwophasestate,
    Anum_pg_subscription_suborigin, GetSubscription, GetSubscriptionRelations,
    Natts_pg_subscription, RemoveSubscriptionRel, Subscription, SubscriptionObjectIndexId,
    SubscriptionRelationId, LOGICALREP_ORIGIN_ANY, LOGICALREP_ORIGIN_NONE,
    LOGICALREP_STREAM_OFF, LOGICALREP_STREAM_ON, LOGICALREP_STREAM_PARALLEL,
    LOGICALREP_TWOPHASE_STATE_DISABLED, LOGICALREP_TWOPHASE_STATE_ENABLED,
    LOGICALREP_TWOPHASE_STATE_PENDING,
};

const SUBOPT_CONNECT: u32 = 0x00000001;
const SUBOPT_ENABLED: u32 = 0x00000002;
const SUBOPT_CREATE_SLOT: u32 = 0x00000004;
const SUBOPT_SLOT_NAME: u32 = 0x00000008;
const SUBOPT_COPY_DATA: u32 = 0x00000010;
const SUBOPT_SYNCHRONOUS_COMMIT: u32 = 0x00000020;
const SUBOPT_REFRESH: u32 = 0x00000040;
const SUBOPT_BINARY: u32 = 0x00000080;
const SUBOPT_STREAMING: u32 = 0x00000100;
const SUBOPT_TWOPHASE_COMMIT: u32 = 0x00000200;
const SUBOPT_DISABLE_ON_ERR: u32 = 0x00000400;
const SUBOPT_PASSWORD_REQUIRED: u32 = 0x00000800;
const SUBOPT_RUN_AS_OWNER: u32 = 0x00001000;
const SUBOPT_FAILOVER: u32 = 0x00002000;
const SUBOPT_LSN: u32 = 0x00004000;
const SUBOPT_ORIGIN: u32 = 0x00008000;

const ROLE_PG_CREATE_SUBSCRIPTION: Oid = 6304;

fn is_set(val: u32, bits: u32) -> bool {
    (val & bits) == bits
}

fn err(msg: impl Into<String>, sqlstate: SqlState) -> Box<PgError> {
    Box::new(PgError::error(msg.into()).with_sqlstate(sqlstate))
}

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

fn conflicting_def_elem(defel: &DefElem<'_>) -> Box<PgError> {
    let mut e = PgError::error("conflicting or redundant options")
        .with_sqlstate(ERRCODE_SYNTAX_ERROR);
    if defel.location >= 0 {
        e.cursor_position = Some(defel.location + 1);
    }
    Box::new(e)
}

fn getattr(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>, attno: i32) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: tup is a catalog row read under its relation's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attno, td, &mut isnull) };
    (d, isnull)
}

fn text_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    let img = varlena::cstring_to_text(mcx, s.as_bytes())?.into_image().leak();
    Ok(Datum::from_usize(img.as_ptr() as usize))
}

struct SubOpts<'mcx> {
    specified_opts: u32,
    slot_name: Option<&'mcx str>,
    synchronous_commit: Option<&'mcx str>,
    connect: bool,
    enabled: bool,
    create_slot: bool,
    copy_data: bool,
    refresh: bool,
    binary: bool,
    streaming: u8,
    twophase: bool,
    disableonerr: bool,
    passwordrequired: bool,
    runasowner: bool,
    failover: bool,
    origin: &'mcx str,
    lsn: XLogRecPtr,
}

fn ReplicationSlotValidateName(name: &str) -> PgResult<()> {
    if name.is_empty() {
        return Err(err(
            format!("replication slot name \"{name}\" is too short"),
            ERRCODE_INVALID_NAME,
        ));
    }
    if name.len() >= NAMEDATALEN as usize {
        return Err(err(
            format!("replication slot name \"{name}\" is too long"),
            ERRCODE_NAME_TOO_LONG,
        ));
    }
    for c in name.bytes() {
        if !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_') {
            return Err(Box::new(
                PgError::error(format!(
                    "replication slot name \"{name}\" contains invalid character"
                ))
                .with_sqlstate(ERRCODE_INVALID_NAME)
                .with_hint(
                    "Replication slot names may only contain lower case letters, numbers, and \
                     the underscore character.",
                ),
            ));
        }
    }
    Ok(())
}

fn defGetStreamingMode(mcx: Mcx<'_>, def: &DefElem<'_>) -> PgResult<u8> {
    let Some(arg) = def.arg else {
        return Ok(LOGICALREP_STREAM_ON);
    };
    if let Some(i) = arg.as_integer() {
        match i.ival {
            0 => return Ok(LOGICALREP_STREAM_OFF),
            1 => return Ok(LOGICALREP_STREAM_ON),
            _ => {}
        }
    } else {
        let sval = commands_define::defGetString(mcx, def)?;
        if sval.eq_ignore_ascii_case("false") || sval.eq_ignore_ascii_case("off") {
            return Ok(LOGICALREP_STREAM_OFF);
        }
        if sval.eq_ignore_ascii_case("true") || sval.eq_ignore_ascii_case("on") {
            return Ok(LOGICALREP_STREAM_ON);
        }
        if sval.eq_ignore_ascii_case("parallel") {
            return Ok(LOGICALREP_STREAM_PARALLEL);
        }
    }
    Err(err(
        format!(
            "{} requires a Boolean value or \"parallel\"",
            def.defname.unwrap_or("")
        ),
        ERRCODE_SYNTAX_ERROR,
    ))
}

fn parse_subscription_options<'mcx>(
    mcx: Mcx<'mcx>,
    stmt_options: &NodeList<'mcx>,
    supported_opts: u32,
) -> PgResult<SubOpts<'mcx>> {
    let mut opts = SubOpts {
        specified_opts: 0,
        slot_name: None,
        synchronous_commit: None,
        connect: is_set(supported_opts, SUBOPT_CONNECT),
        enabled: is_set(supported_opts, SUBOPT_ENABLED),
        create_slot: is_set(supported_opts, SUBOPT_CREATE_SLOT),
        copy_data: is_set(supported_opts, SUBOPT_COPY_DATA),
        refresh: is_set(supported_opts, SUBOPT_REFRESH),
        binary: false,
        streaming: if is_set(supported_opts, SUBOPT_STREAMING) {
            LOGICALREP_STREAM_PARALLEL
        } else {
            0
        },
        twophase: false,
        disableonerr: false,
        passwordrequired: is_set(supported_opts, SUBOPT_PASSWORD_REQUIRED),
        runasowner: false,
        failover: false,
        origin: if is_set(supported_opts, SUBOPT_ORIGIN) {
            LOGICALREP_ORIGIN_ANY
        } else {
            ""
        },
        lsn: InvalidXLogRecPtr,
    };

    for node in stmt_options.iter() {
        let defel = node.as_def_elem().expect("subscription options are DefElems");
        let defname = defel.defname.unwrap_or("");

        if is_set(supported_opts, SUBOPT_CONNECT) && defname == "connect" {
            if is_set(opts.specified_opts, SUBOPT_CONNECT) {
                return Err(conflicting_def_elem(defel));
            }
            opts.specified_opts |= SUBOPT_CONNECT;
            opts.connect = commands_define::defGetBoolean(defel)?;
        } else if is_set(supported_opts, SUBOPT_ENABLED) && defname == "enabled" {
            if is_set(opts.specified_opts, SUBOPT_ENABLED) {
                return Err(conflicting_def_elem(defel));
            }
            opts.specified_opts |= SUBOPT_ENABLED;
            opts.enabled = commands_define::defGetBoolean(defel)?;
        } else if is_set(supported_opts, SUBOPT_CREATE_SLOT) && defname == "create_slot" {
            if is_set(opts.specified_opts, SUBOPT_CREATE_SLOT) {
                return Err(conflicting_def_elem(defel));
            }
            opts.specified_opts |= SUBOPT_CREATE_SLOT;
            opts.create_slot = commands_define::defGetBoolean(defel)?;
        } else if is_set(supported_opts, SUBOPT_SLOT_NAME) && defname == "slot_name" {
            if is_set(opts.specified_opts, SUBOPT_SLOT_NAME) {
                return Err(conflicting_def_elem(defel));
            }
            opts.specified_opts |= SUBOPT_SLOT_NAME;
            let name = commands_define::defGetString(mcx, defel)?;
            if name == "none" {
                opts.slot_name = None;
            } else {
                ReplicationSlotValidateName(name)?;
                opts.slot_name = Some(name);
            }
        } else if is_set(supported_opts, SUBOPT_COPY_DATA) && defname == "copy_data" {
            if is_set(opts.specified_opts, SUBOPT_COPY_DATA) {
                return Err(conflicting_def_elem(defel));
            }
            opts.specified_opts |= SUBOPT_COPY_DATA;
            opts.copy_data = commands_define::defGetBoolean(defel)?;
        } else if is_set(supported_opts, SUBOPT_SYNCHRONOUS_COMMIT)
            && defname == "synchronous_commit"
        {
            if is_set(opts.specified_opts, SUBOPT_SYNCHRONOUS_COMMIT) {
                return Err(conflicting_def_elem(defel));
            }
            opts.specified_opts |= SUBOPT_SYNCHRONOUS_COMMIT;
            let val = commands_define::defGetString(mcx, defel)?;
            opts.synchronous_commit = Some(val);
            guc::set_config_option(
                "synchronous_commit",
                Some(val),
                PGC_BACKEND,
                PGC_S_TEST,
                GUC_ACTION_SET,
                false,
                ErrorLevel(0),
                false,
            )?;
        } else if is_set(supported_opts, SUBOPT_REFRESH) && defname == "refresh" {
            if is_set(opts.specified_opts, SUBOPT_REFRESH) {
                return Err(conflicting_def_elem(defel));
            }
            opts.specified_opts |= SUBOPT_REFRESH;
            opts.refresh = commands_define::defGetBoolean(defel)?;
        } else if is_set(supported_opts, SUBOPT_BINARY) && defname == "binary" {
            if is_set(opts.specified_opts, SUBOPT_BINARY) {
                return Err(conflicting_def_elem(defel));
            }
            opts.specified_opts |= SUBOPT_BINARY;
            opts.binary = commands_define::defGetBoolean(defel)?;
        } else if is_set(supported_opts, SUBOPT_STREAMING) && defname == "streaming" {
            if is_set(opts.specified_opts, SUBOPT_STREAMING) {
                return Err(conflicting_def_elem(defel));
            }
            opts.specified_opts |= SUBOPT_STREAMING;
            opts.streaming = defGetStreamingMode(mcx, defel)?;
        } else if is_set(supported_opts, SUBOPT_TWOPHASE_COMMIT) && defname == "two_phase" {
            if is_set(opts.specified_opts, SUBOPT_TWOPHASE_COMMIT) {
                return Err(conflicting_def_elem(defel));
            }
            opts.specified_opts |= SUBOPT_TWOPHASE_COMMIT;
            opts.twophase = commands_define::defGetBoolean(defel)?;
        } else if is_set(supported_opts, SUBOPT_DISABLE_ON_ERR) && defname == "disable_on_error" {
            if is_set(opts.specified_opts, SUBOPT_DISABLE_ON_ERR) {
                return Err(conflicting_def_elem(defel));
            }
            opts.specified_opts |= SUBOPT_DISABLE_ON_ERR;
            opts.disableonerr = commands_define::defGetBoolean(defel)?;
        } else if is_set(supported_opts, SUBOPT_PASSWORD_REQUIRED)
            && defname == "password_required"
        {
            if is_set(opts.specified_opts, SUBOPT_PASSWORD_REQUIRED) {
                return Err(conflicting_def_elem(defel));
            }
            opts.specified_opts |= SUBOPT_PASSWORD_REQUIRED;
            opts.passwordrequired = commands_define::defGetBoolean(defel)?;
        } else if is_set(supported_opts, SUBOPT_RUN_AS_OWNER) && defname == "run_as_owner" {
            if is_set(opts.specified_opts, SUBOPT_RUN_AS_OWNER) {
                return Err(conflicting_def_elem(defel));
            }
            opts.specified_opts |= SUBOPT_RUN_AS_OWNER;
            opts.runasowner = commands_define::defGetBoolean(defel)?;
        } else if is_set(supported_opts, SUBOPT_FAILOVER) && defname == "failover" {
            if is_set(opts.specified_opts, SUBOPT_FAILOVER) {
                return Err(conflicting_def_elem(defel));
            }
            opts.specified_opts |= SUBOPT_FAILOVER;
            opts.failover = commands_define::defGetBoolean(defel)?;
        } else if is_set(supported_opts, SUBOPT_ORIGIN) && defname == "origin" {
            if is_set(opts.specified_opts, SUBOPT_ORIGIN) {
                return Err(conflicting_def_elem(defel));
            }
            opts.specified_opts |= SUBOPT_ORIGIN;
            let val = commands_define::defGetString(mcx, defel)?;
            if !val.eq_ignore_ascii_case(LOGICALREP_ORIGIN_NONE)
                && !val.eq_ignore_ascii_case(LOGICALREP_ORIGIN_ANY)
            {
                return Err(err(
                    format!("unrecognized origin value: \"{val}\""),
                    ERRCODE_INVALID_PARAMETER_VALUE,
                ));
            }
            opts.origin = val;
        } else if is_set(supported_opts, SUBOPT_LSN) && defname == "lsn" {
            let lsn_str = commands_define::defGetString(mcx, defel)?;
            if is_set(opts.specified_opts, SUBOPT_LSN) {
                return Err(conflicting_def_elem(defel));
            }
            let lsn = if lsn_str == "none" {
                InvalidXLogRecPtr
            } else {
                let lsn = adt_pg_lsn::pg_lsn_in(lsn_str, None)?;
                if lsn == InvalidXLogRecPtr {
                    return Err(err(
                        format!("invalid WAL location (LSN): {lsn_str}"),
                        ERRCODE_INVALID_PARAMETER_VALUE,
                    ));
                }
                lsn
            };
            opts.specified_opts |= SUBOPT_LSN;
            opts.lsn = lsn;
        } else {
            return Err(err(
                format!("unrecognized subscription parameter: \"{defname}\""),
                ERRCODE_SYNTAX_ERROR,
            ));
        }
    }

    if !opts.connect && is_set(supported_opts, SUBOPT_CONNECT) {
        let excl = |a: &str, b: &str| {
            err(
                format!("{a} and {b} are mutually exclusive options"),
                ERRCODE_SYNTAX_ERROR,
            )
        };
        if opts.enabled && is_set(opts.specified_opts, SUBOPT_ENABLED) {
            return Err(excl("connect = false", "enabled = true"));
        }
        if opts.create_slot && is_set(opts.specified_opts, SUBOPT_CREATE_SLOT) {
            return Err(excl("connect = false", "create_slot = true"));
        }
        if opts.copy_data && is_set(opts.specified_opts, SUBOPT_COPY_DATA) {
            return Err(excl("connect = false", "copy_data = true"));
        }

        opts.enabled = false;
        opts.create_slot = false;
        opts.copy_data = false;
    }

    if opts.slot_name.is_none() && is_set(opts.specified_opts, SUBOPT_SLOT_NAME) {
        if opts.enabled {
            if is_set(opts.specified_opts, SUBOPT_ENABLED) {
                return Err(err(
                    "slot_name = NONE and enabled = true are mutually exclusive options",
                    ERRCODE_SYNTAX_ERROR,
                ));
            }
            return Err(err(
                "subscription with slot_name = NONE must also set enabled = false",
                ERRCODE_SYNTAX_ERROR,
            ));
        }
        if opts.create_slot {
            if is_set(opts.specified_opts, SUBOPT_CREATE_SLOT) {
                return Err(err(
                    "slot_name = NONE and create_slot = true are mutually exclusive options",
                    ERRCODE_SYNTAX_ERROR,
                ));
            }
            return Err(err(
                "subscription with slot_name = NONE must also set create_slot = false",
                ERRCODE_SYNTAX_ERROR,
            ));
        }
    }

    Ok(opts)
}

fn publication_list_to_array<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[&str],
) -> PgResult<(Datum, PgVec<'mcx, u8>)> {
    check_duplicates(names)?;
    let mut datums: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, names.len())?;
    for name in names {
        datums.push(text_datum(mcx, name)?);
    }
    let img = datum::array_build::construct_array_image(mcx, &datums, TEXTOID, -1, false, b'i')?;
    Ok((Datum::from_usize(img.as_ptr() as usize), img))
}

fn check_duplicates(names: &[&str]) -> PgResult<()> {
    for (i, name) in names.iter().enumerate() {
        for prev in &names[..i] {
            if prev == name {
                return Err(err(
                    format!("publication name \"{prev}\" used more than once"),
                    ERRCODE_DUPLICATE_OBJECT,
                ));
            }
        }
    }
    Ok(())
}

fn publist_names<'mcx>(mcx: Mcx<'mcx>, list: &NodeList<'mcx>) -> PgResult<PgVec<'mcx, &'mcx str>> {
    let mut names: PgVec<'mcx, &'mcx str> = mcx::vec_with_capacity_in(mcx, list.len())?;
    for node in list.iter() {
        names.push(node.as_string().expect("publication names are Strings").sval);
    }
    Ok(names)
}

fn merge_publications<'mcx>(
    mcx: Mcx<'mcx>,
    oldpublist: &[&'mcx str],
    newlist: &NodeList<'mcx>,
    addpub: bool,
    subname: &str,
) -> PgResult<PgVec<'mcx, &'mcx str>> {
    let mut merged: PgVec<'mcx, &'mcx str> = PgVec::new_in(mcx);
    merged.extend(oldpublist.iter().copied());

    let newnames = publist_names(mcx, newlist)?;
    check_duplicates(&newnames)?;

    for name in newnames.iter() {
        let found = merged.iter().position(|p| p == name);
        match found {
            Some(idx) => {
                if addpub {
                    return Err(err(
                        format!(
                            "publication \"{name}\" is already in subscription \"{subname}\""
                        ),
                        ERRCODE_DUPLICATE_OBJECT,
                    ));
                }
                merged.remove(idx);
            }
            None => {
                if addpub {
                    merged.push(name);
                } else {
                    return Err(err(
                        format!("publication \"{name}\" is not in subscription \"{subname}\""),
                        ERRCODE_INVALID_OBJECT_DEFINITION,
                    ));
                }
            }
        }
    }

    if merged.is_empty() {
        return Err(err(
            "cannot drop all the publications from a subscription",
            ERRCODE_INVALID_OBJECT_DEFINITION,
        ));
    }
    Ok(merged)
}

pub fn CreateSubscription<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateSubscriptionStmt<'mcx>,
    is_top_level: bool,
) -> PgResult<ObjectAddress> {
    let subname = stmt.subname.expect("grammar supplies subname");
    let conninfo = stmt.conninfo.expect("grammar supplies conninfo");
    let owner = miscinit::GetUserId();
    let db = init_small::globals::MyDatabaseId();

    let supported_opts = SUBOPT_CONNECT
        | SUBOPT_ENABLED
        | SUBOPT_CREATE_SLOT
        | SUBOPT_SLOT_NAME
        | SUBOPT_COPY_DATA
        | SUBOPT_SYNCHRONOUS_COMMIT
        | SUBOPT_BINARY
        | SUBOPT_STREAMING
        | SUBOPT_TWOPHASE_COMMIT
        | SUBOPT_DISABLE_ON_ERR
        | SUBOPT_PASSWORD_REQUIRED
        | SUBOPT_RUN_AS_OWNER
        | SUBOPT_FAILOVER
        | SUBOPT_ORIGIN;
    let opts = parse_subscription_options(mcx, &stmt.options, supported_opts)?;

    if opts.create_slot {
        xact::PreventInTransactionBlock(
            is_top_level,
            "CREATE SUBSCRIPTION ... WITH (create_slot = true)",
        )?;
    }

    if !adt_acl::has_privs_of_role(owner, ROLE_PG_CREATE_SUBSCRIPTION)? {
        return Err(Box::new(
            PgError::error("permission denied to create subscription")
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .with_detail(
                    "Only roles with privileges of the \"pg_create_subscription\" role may \
                     create subscriptions.",
                ),
        ));
    }

    let aclresult = aclchk::object_aclcheck(DATABASE_RELATION_ID, db, owner, ACL_CREATE)?;
    if aclresult != ACLCHECK_OK {
        let dbname = dbcommands::get_database_name(db)?.unwrap_or_default();
        aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_DATABASE, &dbname)?;
    }

    if !opts.passwordrequired && !superuser::superuser_arg(owner)? {
        return Err(password_required_superuser_only());
    }

    let rel = table::table_open(mcx, SubscriptionRelationId, RowExclusiveLock)?;

    let existing = GetSysCacheOid(
        SUBSCRIPTIONNAME,
        Anum_pg_subscription_oid,
        SysCacheKey::Value(Datum::from_oid(db)),
        SysCacheKey::Str(subname),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?;
    if existing != InvalidOid {
        return Err(err(
            format!("subscription \"{subname}\" already exists"),
            ERRCODE_DUPLICATE_OBJECT,
        ));
    }

    let slot_name = if !is_set(opts.specified_opts, SUBOPT_SLOT_NAME) && opts.slot_name.is_none()
    {
        Some(subname)
    } else {
        opts.slot_name
    };
    let synchronous_commit = opts.synchronous_commit.unwrap_or("off");

    conninfo::walrcv_check_conninfo(
        mcx,
        conninfo,
        opts.passwordrequired && !superuser::superuser()?,
    )?;

    let mut values = [Datum::null(); Natts_pg_subscription];
    let mut nulls = [false; Natts_pg_subscription];

    let subid = catalog::GetNewOidWithIndex(
        mcx,
        &rel,
        SubscriptionObjectIndexId,
        Anum_pg_subscription_oid as AttrNumber,
    )?;
    let set = |values: &mut [Datum], anum: i32, d: Datum| values[(anum - 1) as usize] = d;

    let mut subname_buf = NameData::default();
    subname_buf.namestrcpy(subname);
    let mut slotname_buf = NameData::default();

    set(&mut values, Anum_pg_subscription_oid, Datum::from_oid(subid));
    set(&mut values, Anum_pg_subscription_subdbid, Datum::from_oid(db));
    set(&mut values, Anum_pg_subscription_subskiplsn, Datum::from_u64(InvalidXLogRecPtr));
    set(
        &mut values,
        Anum_pg_subscription_subname,
        Datum::from_usize(subname_buf.data.as_ptr() as usize),
    );
    set(&mut values, Anum_pg_subscription_subowner, Datum::from_oid(owner));
    set(&mut values, Anum_pg_subscription_subenabled, Datum::from_bool(opts.enabled));
    set(&mut values, Anum_pg_subscription_subbinary, Datum::from_bool(opts.binary));
    set(&mut values, Anum_pg_subscription_substream, Datum::from_char(opts.streaming as i8));
    set(
        &mut values,
        Anum_pg_subscription_subtwophasestate,
        Datum::from_char(if opts.twophase {
            LOGICALREP_TWOPHASE_STATE_PENDING
        } else {
            LOGICALREP_TWOPHASE_STATE_DISABLED
        } as i8),
    );
    set(&mut values, Anum_pg_subscription_subdisableonerr, Datum::from_bool(opts.disableonerr));
    set(
        &mut values,
        Anum_pg_subscription_subpasswordrequired,
        Datum::from_bool(opts.passwordrequired),
    );
    set(&mut values, Anum_pg_subscription_subrunasowner, Datum::from_bool(opts.runasowner));
    set(&mut values, Anum_pg_subscription_subfailover, Datum::from_bool(opts.failover));
    set(&mut values, Anum_pg_subscription_subconninfo, text_datum(mcx, conninfo)?);
    match slot_name {
        Some(name) => {
            slotname_buf.namestrcpy(name);
            set(
                &mut values,
                Anum_pg_subscription_subslotname,
                Datum::from_usize(slotname_buf.data.as_ptr() as usize),
            );
        }
        None => nulls[(Anum_pg_subscription_subslotname - 1) as usize] = true,
    }
    set(&mut values, Anum_pg_subscription_subsynccommit, text_datum(mcx, synchronous_commit)?);
    let pubnames = publist_names(mcx, &stmt.publication)?;
    let (pub_datum, _pub_img) = publication_list_to_array(mcx, &pubnames)?;
    set(&mut values, Anum_pg_subscription_subpublications, pub_datum);
    set(&mut values, Anum_pg_subscription_suborigin, text_datum(mcx, opts.origin)?);

    let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;

    pg_shdepend::recordDependencyOnOwner(mcx, SubscriptionRelationId, subid, owner)?;

    let originname = origin::ReplicationOriginNameForLogicalRep(subid, InvalidOid);
    origin::replorigin_create(mcx, &originname)?;

    if opts.connect {
        let must_use_password = opts.passwordrequired && !superuser::superuser_arg(owner)?;
        let mut wrconn = match connect::connect(mcx, conninfo, must_use_password, subname)? {
            Err(errmsg) => {
                return Err(err(
                    format!(
                        "subscription \"{subname}\" could not connect to the publisher: {errmsg}"
                    ),
                    ERRCODE_CONNECTION_FAILURE,
                ));
            }
            Ok(conn) => conn,
        };

        // C wraps this in PG_TRY with walrcv_disconnect in the cleanup; the
        // connection drops (closing the socket) on both paths here.
        let pubname_strs: Vec<&str> = pubnames.iter().copied().collect();
        let connected = (|| -> PgResult<()> {
            connect::check_publications(&mut wrconn, &pubname_strs)?;
            connect::check_publications_origin(
                &mut wrconn,
                &pubname_strs,
                opts.copy_data,
                Some(opts.origin),
                subname,
            )?;

            // Set sync state based on whether we were asked to copy data.
            let table_state = if opts.copy_data {
                pg_subscription::SUBREL_STATE_INIT
            } else {
                pg_subscription::SUBREL_STATE_READY
            };

            // Get the table list from the publisher; build local status info.
            let tables = connect::fetch_table_list(&mut wrconn, &pubname_strs)?;
            let ntables = tables.len();
            for (nspname, relname) in tables {
                let rv = rel_vocab::RangeVar {
                    catalogname: None,
                    schemaname: Some(nspname.as_str()),
                    relname: relname.as_str(),
                    inh: true,
                    relpersistence: b'p',
                    location: -1,
                };
                let relid = catalog_namespace::RangeVarGetRelid(
                    &rv,
                    types_rel::AccessShareLock,
                    false,
                )?;
                CheckSubscriptionRelkind(
                    lsyscache::get_rel_relkind(relid)? as u8,
                    &nspname,
                    &relname,
                )?;
                pg_subscription::AddSubscriptionRelState(
                    mcx,
                    subid,
                    relid,
                    table_state,
                    InvalidXLogRecPtr,
                    true,
                )?;
            }

            // If requested, create the permanent slot for the subscription
            // (never with an exported snapshot). two_phase is enabled up
            // front only when it is safe (see the C comment).
            if opts.create_slot {
                let slot = slot_name.expect("create_slot implies slot_name");
                let twophase_enabled = opts.twophase && !opts.copy_data && ntables > 0;
                connect::walrcv_create_slot(&mut wrconn, slot, twophase_enabled, opts.failover)?;
                if twophase_enabled {
                    panic!("unported: UpdateTwoPhaseState (two-phase subscription)");
                }
                let _ = elog::elog(
                    types_error::NOTICE,
                    format!("created replication slot \"{slot}\" on publisher"),
                );
            }
            Ok(())
        })();
        drop(wrconn);
        connected?;
    } else {
        elog::ereport(WARNING)
            .errmsg("subscription was created, but is not connected")
            .errhint(
                "To initiate replication, you must manually create the replication slot, enable \
                 the subscription, and refresh the subscription.",
            )
            .finish(loc("CreateSubscription"))?;
    }

    rel.close(RowExclusiveLock)?;

    pgstat::subscription::pgstat_create_subscription(subid);

    if opts.enabled {
        launcher::ApplyLauncherWakeupAtCommit();
    }

    Ok(ObjectAddress::set(SubscriptionRelationId, subid))
}

fn password_required_superuser_only() -> Box<PgError> {
    Box::new(
        PgError::error("password_required=false is superuser-only")
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .with_hint(
                "Subscriptions with the password_required option set to false may only be \
                 created or modified by the superuser.",
            ),
    )
}

fn CheckAlterSubOption(
    sub: &Subscription<'_>,
    option: &str,
    slot_needs_update: bool,
    is_top_level: bool,
) -> PgResult<()> {
    if sub.enabled {
        return Err(err(
            format!("cannot set option \"{option}\" for enabled subscription"),
            ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
        ));
    }
    if slot_needs_update {
        if sub.slotname.is_none() {
            return Err(err(
                format!(
                    "cannot set option \"{option}\" for a subscription that does not have a \
                     slot name"
                ),
                ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            ));
        }
        xact::PreventInTransactionBlock(
            is_top_level,
            &format!("ALTER SUBSCRIPTION ... SET ({option})"),
        )?;
    }
    Ok(())
}

pub fn AlterSubscription<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterSubscriptionStmt<'mcx>,
    is_top_level: bool,
) -> PgResult<ObjectAddress> {
    use AlterSubscriptionType::*;

    let subname = stmt.subname.expect("grammar supplies subname");
    let db = init_small::globals::MyDatabaseId();

    let rel = table::table_open(mcx, SubscriptionRelationId, RowExclusiveLock)?;

    let Some(tup) = SearchSysCacheCopy(
        mcx,
        SUBSCRIPTIONNAME,
        SysCacheKey::Value(Datum::from_oid(db)),
        SysCacheKey::Str(subname),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?
    else {
        return Err(err(
            format!("subscription \"{subname}\" does not exist"),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    };

    let subid = getattr(rel.descr(), tup.as_tuple(), Anum_pg_subscription_oid).0.as_oid();

    if !aclchk::object_ownercheck(SubscriptionRelationId, subid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(ACLCHECK_NOT_OWNER, ObjectType::OBJECT_SUBSCRIPTION, subname)?;
    }

    let sub = GetSubscription(mcx, subid, false)?.expect("missing_ok=false yields an error");

    if !sub.passwordrequired && !superuser::superuser()? {
        return Err(password_required_superuser_only());
    }

    lmgr::LockSharedObject(SubscriptionRelationId, subid, 0, AccessExclusiveLock)?;

    let mut values = [Datum::null(); Natts_pg_subscription];
    let mut nulls = [false; Natts_pg_subscription];
    let mut replaces = [false; Natts_pg_subscription];
    let mut update_tuple = false;
    let mut update_failover = false;
    let mut update_two_phase = false;
    let mut alter_failover_value = false;
    let mut alter_two_phase_value = false;
    let mut slotname_buf = NameData::default();
    let mut pub_img_keepalive: Option<PgVec<'mcx, u8>> = None;

    match stmt.kind {
        ALTER_SUBSCRIPTION_OPTIONS => {
            let supported_opts = SUBOPT_SLOT_NAME
                | SUBOPT_SYNCHRONOUS_COMMIT
                | SUBOPT_BINARY
                | SUBOPT_STREAMING
                | SUBOPT_TWOPHASE_COMMIT
                | SUBOPT_DISABLE_ON_ERR
                | SUBOPT_PASSWORD_REQUIRED
                | SUBOPT_RUN_AS_OWNER
                | SUBOPT_FAILOVER
                | SUBOPT_ORIGIN;
            let opts = parse_subscription_options(mcx, &stmt.options, supported_opts)?;

            if is_set(opts.specified_opts, SUBOPT_SLOT_NAME) {
                if sub.enabled && opts.slot_name.is_none() {
                    return Err(err(
                        "cannot set slot_name = NONE for enabled subscription",
                        ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                    ));
                }
                match opts.slot_name {
                    Some(name) => {
                        slotname_buf.namestrcpy(name);
                        values[(Anum_pg_subscription_subslotname - 1) as usize] =
                            Datum::from_usize(slotname_buf.data.as_ptr() as usize);
                    }
                    None => nulls[(Anum_pg_subscription_subslotname - 1) as usize] = true,
                }
                replaces[(Anum_pg_subscription_subslotname - 1) as usize] = true;
            }

            if let Some(val) = opts.synchronous_commit {
                values[(Anum_pg_subscription_subsynccommit - 1) as usize] =
                    text_datum(mcx, val)?;
                replaces[(Anum_pg_subscription_subsynccommit - 1) as usize] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_BINARY) {
                values[(Anum_pg_subscription_subbinary - 1) as usize] =
                    Datum::from_bool(opts.binary);
                replaces[(Anum_pg_subscription_subbinary - 1) as usize] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_STREAMING) {
                values[(Anum_pg_subscription_substream - 1) as usize] =
                    Datum::from_char(opts.streaming as i8);
                replaces[(Anum_pg_subscription_substream - 1) as usize] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_DISABLE_ON_ERR) {
                values[(Anum_pg_subscription_subdisableonerr - 1) as usize] =
                    Datum::from_bool(opts.disableonerr);
                replaces[(Anum_pg_subscription_subdisableonerr - 1) as usize] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_PASSWORD_REQUIRED) {
                if !opts.passwordrequired && !superuser::superuser()? {
                    return Err(password_required_superuser_only());
                }
                values[(Anum_pg_subscription_subpasswordrequired - 1) as usize] =
                    Datum::from_bool(opts.passwordrequired);
                replaces[(Anum_pg_subscription_subpasswordrequired - 1) as usize] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_RUN_AS_OWNER) {
                values[(Anum_pg_subscription_subrunasowner - 1) as usize] =
                    Datum::from_bool(opts.runasowner);
                replaces[(Anum_pg_subscription_subrunasowner - 1) as usize] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_TWOPHASE_COMMIT) {
                update_two_phase = !opts.twophase;
                alter_two_phase_value = opts.twophase;

                CheckAlterSubOption(&sub, "two_phase", update_two_phase, is_top_level)?;

                if update_two_phase && is_set(opts.specified_opts, SUBOPT_SLOT_NAME) {
                    return Err(err(
                        "\"slot_name\" and \"two_phase\" cannot be altered at the same time",
                        ERRCODE_SYNTAX_ERROR,
                    ));
                }

                // logicalrep_workers_find: no logical replication workers can
                // exist here, so the worker-running error branch is dead.
                if update_two_phase && sub.twophasestate == LOGICALREP_TWOPHASE_STATE_ENABLED {
                    panic!("unported: LookupGXactBySubid (disabling two_phase on an enabled-state subscription)");
                }

                values[(Anum_pg_subscription_subtwophasestate - 1) as usize] =
                    Datum::from_char(if opts.twophase {
                        LOGICALREP_TWOPHASE_STATE_PENDING
                    } else {
                        LOGICALREP_TWOPHASE_STATE_DISABLED
                    } as i8);
                replaces[(Anum_pg_subscription_subtwophasestate - 1) as usize] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_FAILOVER) {
                update_failover = true;
                alter_failover_value = opts.failover;

                CheckAlterSubOption(&sub, "failover", update_failover, is_top_level)?;

                values[(Anum_pg_subscription_subfailover - 1) as usize] =
                    Datum::from_bool(opts.failover);
                replaces[(Anum_pg_subscription_subfailover - 1) as usize] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_ORIGIN) {
                values[(Anum_pg_subscription_suborigin - 1) as usize] =
                    text_datum(mcx, opts.origin)?;
                replaces[(Anum_pg_subscription_suborigin - 1) as usize] = true;
            }

            update_tuple = true;
        }

        ALTER_SUBSCRIPTION_ENABLED => {
            let opts = parse_subscription_options(mcx, &stmt.options, SUBOPT_ENABLED)?;
            debug_assert!(is_set(opts.specified_opts, SUBOPT_ENABLED));

            if sub.slotname.is_none() && opts.enabled {
                return Err(err(
                    "cannot enable subscription that does not have a slot name",
                    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                ));
            }

            values[(Anum_pg_subscription_subenabled - 1) as usize] =
                Datum::from_bool(opts.enabled);
            replaces[(Anum_pg_subscription_subenabled - 1) as usize] = true;

            if opts.enabled {
                launcher::ApplyLauncherWakeupAtCommit();
            }

            update_tuple = true;
        }

        ALTER_SUBSCRIPTION_CONNECTION => {
            let conninfo = stmt.conninfo.expect("grammar supplies conninfo");
            conninfo::walrcv_check_conninfo(
                mcx,
                conninfo,
                sub.passwordrequired && !sub.ownersuperuser,
            )?;

            values[(Anum_pg_subscription_subconninfo - 1) as usize] = text_datum(mcx, conninfo)?;
            replaces[(Anum_pg_subscription_subconninfo - 1) as usize] = true;
            update_tuple = true;
        }

        ALTER_SUBSCRIPTION_SET_PUBLICATION => {
            let supported_opts = SUBOPT_COPY_DATA | SUBOPT_REFRESH;
            let opts = parse_subscription_options(mcx, &stmt.options, supported_opts)?;

            let pubnames = publist_names(mcx, &stmt.publication)?;
            let (pub_datum, img) = publication_list_to_array(mcx, &pubnames)?;
            pub_img_keepalive = Some(img);
            values[(Anum_pg_subscription_subpublications - 1) as usize] = pub_datum;
            replaces[(Anum_pg_subscription_subpublications - 1) as usize] = true;

            update_tuple = true;

            if opts.refresh {
                if !sub.enabled {
                    return Err(Box::new(
                        PgError::error(
                            "ALTER SUBSCRIPTION with refresh is not allowed for disabled \
                             subscriptions",
                        )
                        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .with_hint(
                            "Use ALTER SUBSCRIPTION ... SET PUBLICATION ... WITH (refresh = \
                             false).",
                        ),
                    ));
                }

                if sub.twophasestate == LOGICALREP_TWOPHASE_STATE_ENABLED && opts.copy_data {
                    return Err(Box::new(
                        PgError::error(
                            "ALTER SUBSCRIPTION with refresh and copy_data is not allowed when \
                             two_phase is enabled",
                        )
                        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .with_hint(
                            "Use ALTER SUBSCRIPTION ... SET PUBLICATION with refresh = false, \
                             or with copy_data = false, or use DROP/CREATE SUBSCRIPTION.",
                        ),
                    ));
                }

                xact::PreventInTransactionBlock(is_top_level, "ALTER SUBSCRIPTION with refresh")?;

                let pubs: Vec<&str> = pubnames.iter().copied().collect();
                connect::AlterSubscription_refresh(mcx, &sub, opts.copy_data, &pubs, Some(&pubs))?;
            }
        }

        ALTER_SUBSCRIPTION_ADD_PUBLICATION | ALTER_SUBSCRIPTION_DROP_PUBLICATION => {
            let isadd = stmt.kind == ALTER_SUBSCRIPTION_ADD_PUBLICATION;

            let supported_opts = SUBOPT_REFRESH | SUBOPT_COPY_DATA;
            let opts = parse_subscription_options(mcx, &stmt.options, supported_opts)?;

            let publist =
                merge_publications(mcx, &sub.publications, &stmt.publication, isadd, subname)?;
            let (pub_datum, img) = publication_list_to_array(mcx, &publist)?;
            pub_img_keepalive = Some(img);
            values[(Anum_pg_subscription_subpublications - 1) as usize] = pub_datum;
            replaces[(Anum_pg_subscription_subpublications - 1) as usize] = true;

            update_tuple = true;

            if opts.refresh {
                if !sub.enabled {
                    return Err(Box::new(
                        PgError::error(
                            "ALTER SUBSCRIPTION with refresh is not allowed for disabled \
                             subscriptions",
                        )
                        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .with_hint(format!(
                            "Use {} instead.",
                            if isadd {
                                "ALTER SUBSCRIPTION ... ADD PUBLICATION ... WITH (refresh = false)"
                            } else {
                                "ALTER SUBSCRIPTION ... DROP PUBLICATION ... WITH (refresh = false)"
                            }
                        )),
                    ));
                }

                if sub.twophasestate == LOGICALREP_TWOPHASE_STATE_ENABLED && opts.copy_data {
                    return Err(Box::new(
                        PgError::error(
                            "ALTER SUBSCRIPTION with refresh and copy_data is not allowed when \
                             two_phase is enabled",
                        )
                        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .with_hint(format!(
                            "Use {} with refresh = false, or with copy_data = false, or use \
                             DROP/CREATE SUBSCRIPTION.",
                            if isadd {
                                "ALTER SUBSCRIPTION ... ADD PUBLICATION"
                            } else {
                                "ALTER SUBSCRIPTION ... DROP PUBLICATION"
                            }
                        )),
                    ));
                }

                xact::PreventInTransactionBlock(is_top_level, "ALTER SUBSCRIPTION with refresh")?;

                let pubs: Vec<&str> = publist.iter().copied().collect();
                let added: Vec<&str> = if isadd {
                    publist_names(mcx, &stmt.publication)?.iter().copied().collect()
                } else {
                    Vec::new()
                };
                connect::AlterSubscription_refresh(
                    mcx,
                    &sub,
                    opts.copy_data,
                    &pubs,
                    if isadd { Some(&added) } else { None },
                )?;
            }
        }

        ALTER_SUBSCRIPTION_REFRESH => {
            if !sub.enabled {
                return Err(err(
                    "ALTER SUBSCRIPTION ... REFRESH is not allowed for disabled subscriptions",
                    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                ));
            }

            let opts = parse_subscription_options(mcx, &stmt.options, SUBOPT_COPY_DATA)?;

            if sub.twophasestate == LOGICALREP_TWOPHASE_STATE_ENABLED && opts.copy_data {
                return Err(Box::new(
                    PgError::error(
                        "ALTER SUBSCRIPTION ... REFRESH with copy_data is not allowed when \
                         two_phase is enabled",
                    )
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR)
                    .with_hint(
                        "Use ALTER SUBSCRIPTION ... REFRESH with copy_data = false, or use \
                         DROP/CREATE SUBSCRIPTION.",
                    ),
                ));
            }

            xact::PreventInTransactionBlock(is_top_level, "ALTER SUBSCRIPTION ... REFRESH")?;

            let pubs: Vec<&str> = sub.publications.iter().copied().collect();
            connect::AlterSubscription_refresh(mcx, &sub, opts.copy_data, &pubs, None)?;
        }

        ALTER_SUBSCRIPTION_SKIP => {
            let opts = parse_subscription_options(mcx, &stmt.options, SUBOPT_LSN)?;
            debug_assert!(is_set(opts.specified_opts, SUBOPT_LSN));

            if opts.lsn != InvalidXLogRecPtr {
                let originname = origin::ReplicationOriginNameForLogicalRep(subid, InvalidOid);
                let _originid = origin::replorigin_by_name(&originname, false)?;
                // replorigin_get_progress reads shmem state no apply worker
                // ever writes here, so remote_lsn is always invalid and the
                // greater-than-origin-LSN check cannot fire.
            }

            values[(Anum_pg_subscription_subskiplsn - 1) as usize] = Datum::from_u64(opts.lsn);
            replaces[(Anum_pg_subscription_subskiplsn - 1) as usize] = true;

            update_tuple = true;
        }
    }

    if update_tuple {
        let mut new_tup = heaptuple::heap_modify_tuple(
            mcx,
            tup.as_tuple(),
            rel.descr(),
            &values,
            &nulls,
            &replaces,
        )?;
        let otid = tup.as_tuple().t_self;
        catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut new_tup)?;
    }
    drop(pub_img_keepalive);

    // Update the corresponding slot property on the publisher
    // (subscriptioncmds.c: walrcv_connect + walrcv_alter_slot, disconnect in
    // PG_FINALLY — the connection drops on both paths here).
    if update_failover || update_two_phase {
        let must_use_password = sub.passwordrequired && !sub.ownersuperuser;
        let mut wrconn = match connect::connect(
            mcx,
            sub.conninfo.as_str(),
            must_use_password,
            sub.name.as_str(),
        )? {
            Err(errmsg) => {
                return Err(err(
                    format!(
                        "subscription \"{}\" could not connect to the publisher: {errmsg}",
                        sub.name.as_str()
                    ),
                    ERRCODE_CONNECTION_FAILURE,
                ));
            }
            Ok(conn) => conn,
        };
        let slotname = sub
            .slotname
            .as_ref()
            .expect("CheckAlterSubOption verified a slot name")
            .as_str();
        connect::walrcv_alter_slot(
            &mut wrconn,
            slotname,
            update_failover.then_some(alter_failover_value),
            update_two_phase.then_some(alter_two_phase_value),
        )?;
    }

    rel.close(RowExclusiveLock)?;

    // Wake up related replication workers to handle this change quickly
    // (subscriptioncmds.c:1617): a worker idling against a quiet publisher
    // otherwise keeps the pre-ALTER parameters until its next wakeup.
    launcher::LogicalRepWorkersWakeupAtCommit(subid);

    Ok(ObjectAddress::set(SubscriptionRelationId, subid))
}

pub fn DropSubscription<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &DropSubscriptionStmt<'mcx>,
    is_top_level: bool,
) -> PgResult<()> {
    let subname = stmt.subname.expect("grammar supplies subname");
    let db = init_small::globals::MyDatabaseId();

    let rel = table::table_open(mcx, SubscriptionRelationId, RowExclusiveLock)?;

    let Some(tup) = SearchSysCacheCopy(
        mcx,
        SUBSCRIPTIONNAME,
        SysCacheKey::Value(Datum::from_oid(db)),
        SysCacheKey::Str(subname),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?
    else {
        rel.close(NoLock)?;
        if !stmt.missing_ok {
            return Err(err(
                format!("subscription \"{subname}\" does not exist"),
                ERRCODE_UNDEFINED_OBJECT,
            ));
        }
        return elog::ereport(NOTICE)
            .errmsg(format!("subscription \"{subname}\" does not exist, skipping"))
            .finish(loc("DropSubscription"));
    };

    let subid = getattr(rel.descr(), tup.as_tuple(), Anum_pg_subscription_oid).0.as_oid();

    if !aclchk::object_ownercheck(SubscriptionRelationId, subid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(ACLCHECK_NOT_OWNER, ObjectType::OBJECT_SUBSCRIPTION, subname)?;
    }

    lmgr::LockSharedObject(SubscriptionRelationId, subid, 0, AccessExclusiveLock)?;

    let sub = GetSubscription(mcx, subid, false)?.expect("missing_ok=false yields an error");
    let slotname = sub.slotname.as_ref().map(|s| s.as_str());

    if slotname.is_some() {
        xact::PreventInTransactionBlock(is_top_level, "DROP SUBSCRIPTION")?;
    }

    let tid = tup.as_tuple().t_self;
    catalog_indexing::CatalogTupleDelete(&rel, &tid)?;

    // Stop all the subscription workers immediately (new ones can't start:
    // we hold AccessExclusiveLock on the subscription till end of txn), then
    // drop the launcher's last-start entry (subscriptioncmds.c:1739-1767).
    for slot in launcher::logicalrep_workers_find(subid, false) {
        if let Some(w) = launcher::worker_snapshot(slot) {
            launcher::logicalrep_worker_stop(w.subid, w.relid)?;
        }
    }
    launcher::ApplyLauncherForgetWorkerStartTime(subid);

    let rstates = GetSubscriptionRelations(mcx, subid, true)?;
    for rstate in rstates.iter() {
        if rstate.relid == InvalidOid {
            continue;
        }
        let originname = origin::ReplicationOriginNameForLogicalRep(subid, rstate.relid);
        origin::replorigin_drop_by_name(mcx, &originname, true)?;
    }

    pg_shdepend::deleteSharedDependencyRecordsFor(mcx, SubscriptionRelationId, subid, 0)?;

    RemoveSubscriptionRel(mcx, subid, InvalidOid)?;

    let originname = origin::ReplicationOriginNameForLogicalRep(subid, InvalidOid);
    origin::replorigin_drop_by_name(mcx, &originname, true)?;

    pgstat::subscription::pgstat_drop_subscription(subid);

    if slotname.is_none() && rstates.is_empty() {
        return rel.close(NoLock);
    }

    // Drop the slot(s) at the publisher (subscriptioncmds.c:1810). Connection
    // failure with a slot to drop is an ERROR with C's hint.
    let must_use_password = sub.passwordrequired && !superuser::superuser_arg(sub.owner)?;
    let mut wrconn = match connect::connect(mcx, sub.conninfo.as_str(), must_use_password, subname)? {
        Ok(conn) => conn,
        Err(errmsg) => {
            if slotname.is_none() {
                // Only tablesync-origin cleanup was pending; C warns and returns.
                elog::ereport(types_error::WARNING)
                    .errmsg(format!("could not connect to publisher when attempting to drop replication slot: {errmsg}"))
                    .finish(loc("DropSubscription"))?;
                return rel.close(NoLock);
            }
            return Err(err(
                format!("could not connect to publisher when attempting to drop replication slot \"{}\": {errmsg}", slotname.unwrap_or("")),
                ERRCODE_CONNECTION_FAILURE,
            )
            .into());
        }
    };

    let dropped = (|| -> PgResult<()> {
        for rstate in &rstates {
            // Tablesync slots would be dropped here; tablesync states other
            // than READY are refused upstream (round-4 inc E).
            let _ = rstate;
        }
        if let Some(slot) = slotname {
            connect::drop_slot_at_pub_node(&mut wrconn, slot, false)?;
        }
        Ok(())
    })();
    drop(wrconn);
    dropped?;

    rel.close(NoLock)
}

fn AlterSubscriptionOwner_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tup: &heaptuple::HeapTuple<'mcx>,
    new_owner_id: Oid,
) -> PgResult<()> {
    let td = rel.descr();
    let subid = getattr(td, tup.as_tuple(), Anum_pg_subscription_oid).0.as_oid();
    let subowner = getattr(td, tup.as_tuple(), Anum_pg_subscription_subowner).0.as_oid();
    let passwordrequired =
        getattr(td, tup.as_tuple(), Anum_pg_subscription_subpasswordrequired).0.as_bool();

    if subowner == new_owner_id {
        return Ok(());
    }

    if !aclchk::object_ownercheck(SubscriptionRelationId, subid, miscinit::GetUserId())? {
        // SAFETY: a name attr datum addresses NAMEDATALEN in-tuple bytes.
        let name = unsafe {
            core::ptr::read_unaligned(
                getattr(td, tup.as_tuple(), Anum_pg_subscription_subname).0.as_usize()
                    as *const NameData,
            )
        };
        let name_str = core::str::from_utf8(name.name_str()).expect("subname is UTF-8");
        aclchk::aclcheck_error(ACLCHECK_NOT_OWNER, ObjectType::OBJECT_SUBSCRIPTION, name_str)?;
    }

    if !passwordrequired && !superuser::superuser()? {
        return Err(password_required_superuser_only());
    }

    if !adt_acl::member_can_set_role(miscinit::GetUserId(), new_owner_id)? {
        let rolename = miscinit::GetUserNameFromId(mcx, new_owner_id, false)?
            .expect("noerr=false yields an error");
        return Err(err(
            format!("must be able to SET ROLE \"{}\"", rolename.as_str()),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    let db = init_small::globals::MyDatabaseId();
    let aclresult =
        aclchk::object_aclcheck(DATABASE_RELATION_ID, db, miscinit::GetUserId(), ACL_CREATE)?;
    if aclresult != ACLCHECK_OK {
        let dbname = dbcommands::get_database_name(db)?.unwrap_or_default();
        aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_DATABASE, &dbname)?;
    }

    let mut values = [Datum::null(); Natts_pg_subscription];
    let nulls = [false; Natts_pg_subscription];
    let mut replaces = [false; Natts_pg_subscription];
    values[(Anum_pg_subscription_subowner - 1) as usize] = Datum::from_oid(new_owner_id);
    replaces[(Anum_pg_subscription_subowner - 1) as usize] = true;

    let mut new_tup =
        heaptuple::heap_modify_tuple(mcx, tup.as_tuple(), td, &values, &nulls, &replaces)?;
    let otid = tup.as_tuple().t_self;
    catalog_indexing::CatalogTupleUpdate(mcx, rel, &otid, &mut new_tup)?;

    pg_shdepend::changeDependencyOnOwner(mcx, SubscriptionRelationId, subid, new_owner_id)?;

    // Wake up related background processes to handle this change quickly
    // (subscriptioncmds.c:2022-2023).
    launcher::ApplyLauncherWakeupAtCommit();
    launcher::LogicalRepWorkersWakeupAtCommit(subid);

    Ok(())
}

pub fn AlterSubscriptionOwner<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    new_owner_id: Oid,
) -> PgResult<ObjectAddress> {
    let db = init_small::globals::MyDatabaseId();
    let rel = table::table_open(mcx, SubscriptionRelationId, RowExclusiveLock)?;

    let Some(tup) = SearchSysCacheCopy(
        mcx,
        SUBSCRIPTIONNAME,
        SysCacheKey::Value(Datum::from_oid(db)),
        SysCacheKey::Str(name),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?
    else {
        return Err(err(
            format!("subscription \"{name}\" does not exist"),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    };

    let subid = getattr(rel.descr(), tup.as_tuple(), Anum_pg_subscription_oid).0.as_oid();

    AlterSubscriptionOwner_internal(mcx, &rel, &tup, new_owner_id)?;

    rel.close(RowExclusiveLock)?;

    Ok(ObjectAddress::set(SubscriptionRelationId, subid))
}

pub fn AlterSubscriptionOwner_oid<'mcx>(
    mcx: Mcx<'mcx>,
    subid: Oid,
    new_owner_id: Oid,
) -> PgResult<()> {
    let rel = table::table_open(mcx, SubscriptionRelationId, RowExclusiveLock)?;

    let Some(tup) = SearchSysCacheCopy(
        mcx,
        SUBSCRIPTIONOID,
        SysCacheKey::Value(Datum::from_oid(subid)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?
    else {
        return Err(err(
            format!("subscription with OID {subid} does not exist"),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    };

    AlterSubscriptionOwner_internal(mcx, &rel, &tup, new_owner_id)?;

    rel.close(RowExclusiveLock)
}

pub fn init_seams() {
    pg_shdepend::alter_subscription_owner_oid::set(AlterSubscriptionOwner_oid);
}

// CheckSubscriptionRelkind (catalog/pg_subscription.c): logical replication
// targets must be plain or partitioned tables.
fn CheckSubscriptionRelkind(relkind: u8, nspname: &str, relname: &str) -> PgResult<()> {
    // RELKIND_RELATION 'r' / RELKIND_PARTITIONED_TABLE 'p'.
    if relkind != b'r' && relkind != b'p' {
        return Err(err(
            format!("cannot use relation \"{nspname}.{relname}\" as logical replication target"),
            types_error::ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }
    Ok(())
}
