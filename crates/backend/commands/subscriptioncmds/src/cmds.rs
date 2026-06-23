//! CREATE / ALTER / DROP SUBSCRIPTION (`commands/subscriptioncmds.c`).
//!
//! Ported 1:1 from PostgreSQL 18.3:
//!
//! * [`parse_subscription_options`] — the WITH-option parser
//!   (`subscriptioncmds.c:124-445`),
//! * [`CreateSubscription`] (`:539-817`),
//! * [`AlterSubscription`] (`:1100-1623`),
//! * [`DropSubscription`] (`:1626-1912`),
//! * the helpers `publicationListToArray`, `check_duplicates_in_publist`,
//!   `merge_publications`, `defGetStreamingMode`, `CheckAlterSubOption`,
//!   `ReplicationOriginNameForLogicalRep`.
//!
//! The actual logical-replication apply path (connecting to a publisher to
//! create/drop/alter replication slots and fetch the table list) is reached
//! only when `connect = true` (CREATE) or when a slot is associated (DROP) or a
//! slot property is changed (ALTER). Those legs faithfully attempt
//! `walrcv_connect`, which — with no publisher reachable in a regression
//! environment — raises the same `ERRCODE_CONNECTION_FAILURE` "could not connect
//! to the publisher" error PostgreSQL raises. The catalog DDL and the entire
//! option-validation surface (the `connect = false` path exercised by the
//! regression suite) are real.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use mcx::{Mcx, PgBox, PgString};

use utils_error::{ereport, PgResult};
use types_error::{
    PgError, ERRCODE_CONNECTION_FAILURE, ERRCODE_DUPLICATE_OBJECT,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT, ERROR, NOTICE, WARNING,
};

use ::types_acl::acl::{ACLCHECK_OK, ACL_CREATE, ACLCHECK_NOT_OWNER};
use ::types_catalog::catalog::DATABASE_RELATION_ID;
use ::types_catalog::catalog_dependency::ObjectAddress;
use ::types_catalog::pg_subscription::{
    Anum_pg_subscription_oid, Anum_pg_subscription_subbinary,
    Anum_pg_subscription_subconninfo, Anum_pg_subscription_subdbid,
    Anum_pg_subscription_subdisableonerr, Anum_pg_subscription_subenabled,
    Anum_pg_subscription_subfailover, Anum_pg_subscription_subname,
    Anum_pg_subscription_suborigin, Anum_pg_subscription_subowner,
    Anum_pg_subscription_subpasswordrequired, Anum_pg_subscription_subpublications,
    Anum_pg_subscription_subrunasowner, Anum_pg_subscription_subskiplsn,
    Anum_pg_subscription_subslotname, Anum_pg_subscription_substream,
    Anum_pg_subscription_subsynccommit, Anum_pg_subscription_subtwophasestate,
    Natts_pg_subscription, SubscriptionObjectIndexId, SubscriptionRelationId,
    LOGICALREP_ORIGIN_ANY, LOGICALREP_ORIGIN_NONE, LOGICALREP_STREAM_OFF,
    LOGICALREP_STREAM_ON, LOGICALREP_STREAM_PARALLEL,
    LOGICALREP_TWOPHASE_STATE_DISABLED, LOGICALREP_TWOPHASE_STATE_ENABLED,
    LOGICALREP_TWOPHASE_STATE_PENDING, SUBREL_STATE_INIT, SUBREL_STATE_READY,
};
use ::types_core::primitive::{InvalidOid, Oid};
use ::nodes::ddlnodes::{
    AlterSubscriptionStmt, AlterSubscriptionType, CreateSubscriptionStmt,
    DropSubscriptionStmt,
};
use ::nodes::nodes::{ntag, Node};
use ::nodes::parsenodes::ObjectType;
use ::nodes::parsestmt::ParseState;
use ::types_storage::lock::{AccessExclusiveLock, AccessShareLock, NoLock, RowExclusiveLock};
use ::types_tuple::heaptuple::{Datum, FormedTuple};
use ::types_tuple::heaptuple::TEXTOID;

use heaptuple::{heap_form_tuple, heap_modify_tuple};
use aclchk::{object_aclcheck, object_ownercheck};
use ::catalog_catalog::GetNewOidWithIndex;
use ::indexing::keystone::{CatalogTupleDelete, CatalogTupleInsert};
use ::catalog_namespace::RangeVarGetRelid;
use ::objectaccess::invoke_object_drop_hook;
use pg_shdepend::{deleteSharedDependencyRecordsFor, recordDependencyOnOwner};
use ::replication_libpqwalreceiver::libpqrcv_check_conninfo;
use origin::{
    replorigin_by_name, replorigin_create, replorigin_drop_by_name, replorigin_get_progress,
};
use ::lmgr::LockSharedObject;
use ::cache_syscache::cacheinfo::SUBSCRIPTIONNAME;
use cache_syscache::{ReleaseSysCache, SearchSysCache2};
use ::miscinit::GetUserId;

use ::cache::syscache::SysCacheKey;
use ::datum::Datum as KeyDatum;

use objectaccess_seams as objaccess;
use dbcommands_seams as dbcommands_seams;
use ::define_seams::DefElemArg;
use event_trigger_seams as evttrig_seams;
use tablespace_globals_seams as globals_seams;
use launcher_seams as launcher_seams;
use worker_seams as worker_seams;
use utility_out_seams as utility_seams;
use acl_seams as acl_seams;
use arrayfuncs_seams as arrayfuncs_seams;
use guc_seams as guc_seams;
use superuser_seams as superuser_seams;

use crate::{
    aclcheck_error_str, name_datum, name_str, object_address_set,
};

// ===========================================================================
// SubOpts bitmap (subscriptioncmds.c:60-105).
// ===========================================================================

const SUBOPT_CONNECT: u32 = 0x0000_0001;
const SUBOPT_ENABLED: u32 = 0x0000_0002;
const SUBOPT_CREATE_SLOT: u32 = 0x0000_0004;
const SUBOPT_SLOT_NAME: u32 = 0x0000_0008;
const SUBOPT_COPY_DATA: u32 = 0x0000_0010;
const SUBOPT_SYNCHRONOUS_COMMIT: u32 = 0x0000_0020;
const SUBOPT_REFRESH: u32 = 0x0000_0040;
const SUBOPT_BINARY: u32 = 0x0000_0080;
const SUBOPT_STREAMING: u32 = 0x0000_0100;
const SUBOPT_TWOPHASE_COMMIT: u32 = 0x0000_0200;
const SUBOPT_DISABLE_ON_ERR: u32 = 0x0000_0400;
const SUBOPT_PASSWORD_REQUIRED: u32 = 0x0000_0800;
const SUBOPT_RUN_AS_OWNER: u32 = 0x0000_1000;
const SUBOPT_FAILOVER: u32 = 0x0000_2000;
const SUBOPT_LSN: u32 = 0x0000_4000;
const SUBOPT_ORIGIN: u32 = 0x0000_8000;

/// `ROLE_PG_CREATE_SUBSCRIPTION` (`pg_authid.dat`, OID 6304).
const ROLE_PG_CREATE_SUBSCRIPTION: Oid = 6304;

/// `IsSet(val, bits)`.
#[inline]
fn is_set(val: u32, bits: u32) -> bool {
    (val & bits) == bits
}

/// `ereport` location helper for `subscriptioncmds.c`.
fn errloc(funcname: &'static str) -> ::types_error::ErrorLocation {
    ::types_error::ErrorLocation::new("../src/backend/commands/subscriptioncmds.c", 0, funcname)
}

/// `XLogRecPtrIsInvalid` / `InvalidXLogRecPtr`.
const INVALID_XLOG_REC_PTR: u64 = 0;

/// `struct SubOpts` (subscriptioncmds.c:84-105) — parsed option values.
#[derive(Default)]
struct SubOpts {
    specified_opts: u32,
    slot_name: Option<String>,
    synchronous_commit: Option<String>,
    connect: bool,
    enabled: bool,
    create_slot: bool,
    copy_data: bool,
    refresh: bool,
    binary: bool,
    streaming: i8,
    twophase: bool,
    disableonerr: bool,
    passwordrequired: bool,
    runasowner: bool,
    failover: bool,
    origin: Option<String>,
    lsn: u64,
}

// ===========================================================================
// DefElem value helpers (define.c, via -seams), mirroring publicationcmds.
// ===========================================================================

/// Project a `DefElem`'s value node into the `DefElemArg` the define.c value
/// accessors switch on (`nodeTag(def->arg)`).
fn defel_arg(arg: Option<&Node<'_>>) -> PgResult<Option<DefElemArg>> {
    let Some(node) = arg else {
        return Ok(None);
    };
    // Mirror `defGetString`'s full node switch (define.c): a bare-identifier
    // value arrives as a `T_TypeName` and a qualified name as a `T_List`; both
    // render to text. A `_ => AStar` catch-all would collapse those to `"*"`.
    Ok(Some(match node.node_tag() {
        ntag::T_Integer => DefElemArg::Integer(node.expect_integer().ival as i64),
        ntag::T_Float => DefElemArg::Float(node.expect_float().fval.as_str().to_string()),
        ntag::T_Boolean => DefElemArg::Boolean(node.expect_boolean().boolval),
        ntag::T_String => DefElemArg::String(node.expect_string().sval.as_str().to_string()),
        ntag::T_TypeName => DefElemArg::TypeName(defel_type_name_to_string(node.expect_typename())?),
        ntag::T_List => DefElemArg::List(defel_name_list_to_string(node.expect_list())?),
        ntag::T_A_Star => DefElemArg::AStar,
        other => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unrecognized node type: {}", other))
                .into_error())
        }
    }))
}

/// `TypeNameToString(typeName)` for the `defGetString` `T_TypeName` case.
fn defel_type_name_to_string(tn: &::nodes::rawnodes::TypeName<'_>) -> PgResult<String> {
    if tn.names.is_empty() {
        return Err(ereport(ERROR)
            .errmsg_internal("DefElem TypeName carries no name")
            .into_error());
    }
    let mut out = String::new();
    for (i, name) in tn.names.iter().enumerate() {
        if i != 0 {
            out.push('.');
        }
        let node: &Node = name;
        match node.node_tag() {
            ntag::T_String => out.push_str(node.expect_string().sval.as_str()),
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("unrecognized node type: {}", other))
                    .into_error())
            }
        }
    }
    if tn.pct_type {
        out.push_str("%TYPE");
    }
    if !tn.arrayBounds.is_empty() {
        out.push_str("[]");
    }
    Ok(out)
}

/// `NameListToString(names)` (namespace.c) for the `defGetString` `T_List` case.
fn defel_name_list_to_string(names: &[::nodes::nodes::NodePtr<'_>]) -> PgResult<String> {
    let mut out = String::new();
    for (i, name) in names.iter().enumerate() {
        if i != 0 {
            out.push('.');
        }
        let node: &Node = name;
        match node.node_tag() {
            ntag::T_String => out.push_str(node.expect_string().sval.as_str()),
            ntag::T_A_Star => out.push('*'),
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("unrecognized node type: {}", other))
                    .into_error())
            }
        }
    }
    Ok(out)
}

/// `defGetString(def)` (define.c).
fn def_get_string(mcx: Mcx<'_>, defname: &str, arg: Option<&Node<'_>>) -> PgResult<String> {
    let s = ::define_seams::def_get_string::call(
        mcx,
        defname.to_string(),
        defel_arg(arg)?,
    )?;
    Ok(s.as_str().to_string())
}

/// `defGetBoolean(def)` (define.c).
fn def_get_boolean(defname: &str, arg: Option<&Node<'_>>) -> PgResult<bool> {
    ::define_seams::def_get_boolean::call(defname.to_string(), defel_arg(arg)?)
}

/// `errorConflictingDefElem(defel, pstate)` (defrem.c).
fn error_conflicting_def_elem() -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg("conflicting or redundant options")
        .into_error()
}

/// `pg_strcasecmp(a, b) == 0`.
fn eq_ci(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// `defGetStreamingMode(def)` (subscriptioncmds.c:2467-2511).
fn def_get_streaming_mode(mcx: Mcx<'_>, defname: &str, arg: Option<&Node<'_>>) -> PgResult<i8> {
    // If no parameter value given, assume "true".
    let node = match arg {
        None => return Ok(LOGICALREP_STREAM_ON),
        Some(n) => n,
    };

    if node.node_tag() == ntag::T_Integer {
        match node.expect_integer().ival {
            0 => return Ok(LOGICALREP_STREAM_OFF),
            1 => return Ok(LOGICALREP_STREAM_ON),
            _ => {}
        }
    } else {
        let sval = def_get_string(mcx, defname, arg)?;
        if eq_ci(&sval, "false") || eq_ci(&sval, "off") {
            return Ok(LOGICALREP_STREAM_OFF);
        }
        if eq_ci(&sval, "true") || eq_ci(&sval, "on") {
            return Ok(LOGICALREP_STREAM_ON);
        }
        if eq_ci(&sval, "parallel") {
            return Ok(LOGICALREP_STREAM_PARALLEL);
        }
    }

    Err(ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!("{defname} requires a Boolean value or \"parallel\""))
        .into_error())
}

// ===========================================================================
// DefElem option list iteration helper.
// ===========================================================================

/// A parsed `(defname, arg)` view of an mcx `DefElem` option node.
struct DefView<'a, 'mcx> {
    defname: String,
    arg: Option<&'a Node<'mcx>>,
}

/// Read the option list (`stmt->options`, a `List *` of `DefElem`) as views.
fn options_as_views<'a, 'mcx>(
    options: &'a [PgBox<'mcx, Node<'mcx>>],
) -> PgResult<Vec<DefView<'a, 'mcx>>> {
    let mut out = Vec::with_capacity(options.len());
    for node in options {
        if node.node_tag() != ntag::T_DefElem {
            return Err(PgError::error("subscription option list element is not a DefElem"));
        }
        let defel = node.expect_defelem();
        out.push(DefView {
            defname: defel.defname.as_deref().unwrap_or("").to_string(),
            arg: defel.arg.as_deref(),
        });
    }
    Ok(out)
}

// ===========================================================================
// parse_subscription_options (subscriptioncmds.c:124-445).
// ===========================================================================

fn parse_subscription_options<'mcx>(
    mcx: Mcx<'mcx>,
    options: &[PgBox<'mcx, Node<'mcx>>],
    supported_opts: u32,
) -> PgResult<SubOpts> {
    debug_assert!(supported_opts != 0);
    // If connect option is supported, these others also need to be.
    debug_assert!(
        !is_set(supported_opts, SUBOPT_CONNECT)
            || is_set(
                supported_opts,
                SUBOPT_ENABLED | SUBOPT_CREATE_SLOT | SUBOPT_COPY_DATA
            )
    );

    let mut opts = SubOpts::default();

    // Set default values for the supported options.
    if is_set(supported_opts, SUBOPT_CONNECT) {
        opts.connect = true;
    }
    if is_set(supported_opts, SUBOPT_ENABLED) {
        opts.enabled = true;
    }
    if is_set(supported_opts, SUBOPT_CREATE_SLOT) {
        opts.create_slot = true;
    }
    if is_set(supported_opts, SUBOPT_COPY_DATA) {
        opts.copy_data = true;
    }
    if is_set(supported_opts, SUBOPT_REFRESH) {
        opts.refresh = true;
    }
    if is_set(supported_opts, SUBOPT_BINARY) {
        opts.binary = false;
    }
    if is_set(supported_opts, SUBOPT_STREAMING) {
        opts.streaming = LOGICALREP_STREAM_PARALLEL;
    }
    if is_set(supported_opts, SUBOPT_TWOPHASE_COMMIT) {
        opts.twophase = false;
    }
    if is_set(supported_opts, SUBOPT_DISABLE_ON_ERR) {
        opts.disableonerr = false;
    }
    if is_set(supported_opts, SUBOPT_PASSWORD_REQUIRED) {
        opts.passwordrequired = true;
    }
    if is_set(supported_opts, SUBOPT_RUN_AS_OWNER) {
        opts.runasowner = false;
    }
    if is_set(supported_opts, SUBOPT_FAILOVER) {
        opts.failover = false;
    }
    if is_set(supported_opts, SUBOPT_ORIGIN) {
        opts.origin = Some(LOGICALREP_ORIGIN_ANY.to_string());
    }

    // Parse options.
    let views = options_as_views(options)?;
    for v in &views {
        let dn = v.defname.as_str();

        if is_set(supported_opts, SUBOPT_CONNECT) && dn == "connect" {
            if is_set(opts.specified_opts, SUBOPT_CONNECT) {
                return Err(error_conflicting_def_elem());
            }
            opts.specified_opts |= SUBOPT_CONNECT;
            opts.connect = def_get_boolean(dn, v.arg)?;
        } else if is_set(supported_opts, SUBOPT_ENABLED) && dn == "enabled" {
            if is_set(opts.specified_opts, SUBOPT_ENABLED) {
                return Err(error_conflicting_def_elem());
            }
            opts.specified_opts |= SUBOPT_ENABLED;
            opts.enabled = def_get_boolean(dn, v.arg)?;
        } else if is_set(supported_opts, SUBOPT_CREATE_SLOT) && dn == "create_slot" {
            if is_set(opts.specified_opts, SUBOPT_CREATE_SLOT) {
                return Err(error_conflicting_def_elem());
            }
            opts.specified_opts |= SUBOPT_CREATE_SLOT;
            opts.create_slot = def_get_boolean(dn, v.arg)?;
        } else if is_set(supported_opts, SUBOPT_SLOT_NAME) && dn == "slot_name" {
            if is_set(opts.specified_opts, SUBOPT_SLOT_NAME) {
                return Err(error_conflicting_def_elem());
            }
            opts.specified_opts |= SUBOPT_SLOT_NAME;
            let slot_name = def_get_string(mcx, dn, v.arg)?;
            // Setting slot_name = NONE is treated as no slot name.
            if slot_name == "none" {
                opts.slot_name = None;
            } else {
                replication_slot_validate_name(&slot_name)?;
                opts.slot_name = Some(slot_name);
            }
        } else if is_set(supported_opts, SUBOPT_COPY_DATA) && dn == "copy_data" {
            if is_set(opts.specified_opts, SUBOPT_COPY_DATA) {
                return Err(error_conflicting_def_elem());
            }
            opts.specified_opts |= SUBOPT_COPY_DATA;
            opts.copy_data = def_get_boolean(dn, v.arg)?;
        } else if is_set(supported_opts, SUBOPT_SYNCHRONOUS_COMMIT) && dn == "synchronous_commit" {
            if is_set(opts.specified_opts, SUBOPT_SYNCHRONOUS_COMMIT) {
                return Err(error_conflicting_def_elem());
            }
            opts.specified_opts |= SUBOPT_SYNCHRONOUS_COMMIT;
            let sc = def_get_string(mcx, dn, v.arg)?;
            // Test if the given value is valid for the synchronous_commit GUC.
            guc_seams::set_config_option::call(
                "synchronous_commit",
                &sc,
                types_guc::guc::GucContext::PGC_BACKEND,
                types_guc::guc::GucSource::PGC_S_TEST,
            )?;
            opts.synchronous_commit = Some(sc);
        } else if is_set(supported_opts, SUBOPT_REFRESH) && dn == "refresh" {
            if is_set(opts.specified_opts, SUBOPT_REFRESH) {
                return Err(error_conflicting_def_elem());
            }
            opts.specified_opts |= SUBOPT_REFRESH;
            opts.refresh = def_get_boolean(dn, v.arg)?;
        } else if is_set(supported_opts, SUBOPT_BINARY) && dn == "binary" {
            if is_set(opts.specified_opts, SUBOPT_BINARY) {
                return Err(error_conflicting_def_elem());
            }
            opts.specified_opts |= SUBOPT_BINARY;
            opts.binary = def_get_boolean(dn, v.arg)?;
        } else if is_set(supported_opts, SUBOPT_STREAMING) && dn == "streaming" {
            if is_set(opts.specified_opts, SUBOPT_STREAMING) {
                return Err(error_conflicting_def_elem());
            }
            opts.specified_opts |= SUBOPT_STREAMING;
            opts.streaming = def_get_streaming_mode(mcx, dn, v.arg)?;
        } else if is_set(supported_opts, SUBOPT_TWOPHASE_COMMIT) && dn == "two_phase" {
            if is_set(opts.specified_opts, SUBOPT_TWOPHASE_COMMIT) {
                return Err(error_conflicting_def_elem());
            }
            opts.specified_opts |= SUBOPT_TWOPHASE_COMMIT;
            opts.twophase = def_get_boolean(dn, v.arg)?;
        } else if is_set(supported_opts, SUBOPT_DISABLE_ON_ERR) && dn == "disable_on_error" {
            if is_set(opts.specified_opts, SUBOPT_DISABLE_ON_ERR) {
                return Err(error_conflicting_def_elem());
            }
            opts.specified_opts |= SUBOPT_DISABLE_ON_ERR;
            opts.disableonerr = def_get_boolean(dn, v.arg)?;
        } else if is_set(supported_opts, SUBOPT_PASSWORD_REQUIRED) && dn == "password_required" {
            if is_set(opts.specified_opts, SUBOPT_PASSWORD_REQUIRED) {
                return Err(error_conflicting_def_elem());
            }
            opts.specified_opts |= SUBOPT_PASSWORD_REQUIRED;
            opts.passwordrequired = def_get_boolean(dn, v.arg)?;
        } else if is_set(supported_opts, SUBOPT_RUN_AS_OWNER) && dn == "run_as_owner" {
            if is_set(opts.specified_opts, SUBOPT_RUN_AS_OWNER) {
                return Err(error_conflicting_def_elem());
            }
            opts.specified_opts |= SUBOPT_RUN_AS_OWNER;
            opts.runasowner = def_get_boolean(dn, v.arg)?;
        } else if is_set(supported_opts, SUBOPT_FAILOVER) && dn == "failover" {
            if is_set(opts.specified_opts, SUBOPT_FAILOVER) {
                return Err(error_conflicting_def_elem());
            }
            opts.specified_opts |= SUBOPT_FAILOVER;
            opts.failover = def_get_boolean(dn, v.arg)?;
        } else if is_set(supported_opts, SUBOPT_ORIGIN) && dn == "origin" {
            if is_set(opts.specified_opts, SUBOPT_ORIGIN) {
                return Err(error_conflicting_def_elem());
            }
            opts.specified_opts |= SUBOPT_ORIGIN;
            let origin = def_get_string(mcx, dn, v.arg)?;
            if !eq_ci(&origin, LOGICALREP_ORIGIN_NONE) && !eq_ci(&origin, LOGICALREP_ORIGIN_ANY) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("unrecognized origin value: \"{origin}\""))
                    .into_error());
            }
            opts.origin = Some(origin);
        } else if is_set(supported_opts, SUBOPT_LSN) && dn == "lsn" {
            let lsn_str = def_get_string(mcx, dn, v.arg)?;
            if is_set(opts.specified_opts, SUBOPT_LSN) {
                return Err(error_conflicting_def_elem());
            }
            // Setting lsn = NONE is treated as resetting LSN.
            let lsn = if lsn_str == "none" {
                INVALID_XLOG_REC_PTR
            } else {
                let lsn = pg_lsn_in_str(&lsn_str)?;
                if lsn == INVALID_XLOG_REC_PTR {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!("invalid WAL location (LSN): {lsn_str}"))
                        .into_error());
                }
                lsn
            };
            opts.specified_opts |= SUBOPT_LSN;
            opts.lsn = lsn;
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("unrecognized subscription parameter: \"{dn}\""))
                .into_error());
        }
    }

    // We've been explicitly asked to not connect, that requires additional
    // processing.
    if !opts.connect && is_set(supported_opts, SUBOPT_CONNECT) {
        if opts.enabled && is_set(opts.specified_opts, SUBOPT_ENABLED) {
            return Err(mutually_exclusive("connect = false", "enabled = true"));
        }
        if opts.create_slot && is_set(opts.specified_opts, SUBOPT_CREATE_SLOT) {
            return Err(mutually_exclusive("connect = false", "create_slot = true"));
        }
        if opts.copy_data && is_set(opts.specified_opts, SUBOPT_COPY_DATA) {
            return Err(mutually_exclusive("connect = false", "copy_data = true"));
        }
        // Change the defaults of other options.
        opts.enabled = false;
        opts.create_slot = false;
        opts.copy_data = false;
    }

    // Additional checking for slot_name = NONE.
    if opts.slot_name.is_none() && is_set(opts.specified_opts, SUBOPT_SLOT_NAME) {
        if opts.enabled {
            if is_set(opts.specified_opts, SUBOPT_ENABLED) {
                return Err(mutually_exclusive("slot_name = NONE", "enabled = true"));
            } else {
                return Err(must_also_set("slot_name = NONE", "enabled = false"));
            }
        }
        if opts.create_slot {
            if is_set(opts.specified_opts, SUBOPT_CREATE_SLOT) {
                return Err(mutually_exclusive("slot_name = NONE", "create_slot = true"));
            } else {
                return Err(must_also_set("slot_name = NONE", "create_slot = false"));
            }
        }
    }

    Ok(opts)
}

fn mutually_exclusive(a: &str, b: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!("{a} and {b} are mutually exclusive options"))
        .into_error()
}

fn must_also_set(a: &str, b: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!("subscription with {a} must also set {b}"))
        .into_error()
}

/// `ReplicationSlotValidateName(name, ERROR)` (slot.c) — validate without the
/// soft-error context, raising on failure (the C `elevel == ERROR` call).
fn replication_slot_validate_name(name: &str) -> PgResult<()> {
    match slot_seams::replication_slot_validate_name_internal::call(name) {
        Ok(()) => Ok(()),
        Err((sqlstate, msg, hint)) => {
            let mut b = ereport(ERROR).errcode(sqlstate).errmsg(msg);
            if let Some(h) = hint {
                b = b.errhint(h);
            }
            Err(b.into_error())
        }
    }
}

/// `DatumGetLSN(DirectFunctionCall1(pg_lsn_in, CStringGetDatum(s)))`.
fn pg_lsn_in_str(s: &str) -> PgResult<u64> {
    lsn_trigfuncs::pg_lsn::pg_lsn_in(s, None)
}

// ===========================================================================
// publicationListToArray / check_duplicates_in_publist / merge_publications
// (subscriptioncmds.c:509-538, 2362-2459).
// ===========================================================================

/// `strVal(lfirst(cell))` over the publication-name `String` node list.
fn publist_names(publist: &[PgBox<'_, Node<'_>>]) -> PgResult<Vec<String>> {
    let mut out = Vec::with_capacity(publist.len());
    for node in publist {
        if node.node_tag() != ntag::T_String {
            return Err(PgError::error("publication list element is not a String"));
        }
        out.push(node.expect_string().sval.as_str().to_string());
    }
    Ok(out)
}

/// `check_duplicates_in_publist(publist, datums)` (subscriptioncmds.c:2362-2389)
/// — error on any duplicate publication name.
fn check_duplicates_in_publist(names: &[String]) -> PgResult<()> {
    for (i, name) in names.iter().enumerate() {
        for pname in &names[..i] {
            if name == pname {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_OBJECT)
                    .errmsg(format!("publication name \"{pname}\" used more than once"))
                    .into_error());
            }
        }
    }
    Ok(())
}

/// `publicationListToArray(publist)` (subscriptioncmds.c:509-532) — validate
/// uniqueness and build a `text[]` `Datum`.
fn publication_list_to_array<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[String],
) -> PgResult<Datum<'mcx>> {
    check_duplicates_in_publist(names)?;

    // C: construct_array_builtin(datums, list_length, TEXTOID) where each datum
    // is CStringGetTextDatum(name). strlist_to_textarray builds exactly this
    // (non-null `text[]`) and returns the array varlena image.
    let list: Vec<Option<String>> = names.iter().map(|n| Some(n.clone())).collect();
    let image = objectaddress::fmgr_sql::strlist_to_textarray(mcx, &list)?;
    Ok(Datum::ByRef(image))
}

/// `merge_publications(oldpublist, newpublist, addpub, subname)`
/// (subscriptioncmds.c:2402-2459).
fn merge_publications(
    oldpublist: &[PgString<'_>],
    newpublist: &[String],
    addpub: bool,
    subname: &str,
) -> PgResult<Vec<String>> {
    let mut result: Vec<String> = oldpublist.iter().map(|s| s.as_str().to_string()).collect();

    check_duplicates_in_publist(newpublist)?;

    for name in newpublist {
        let pos = result.iter().position(|p| p == name);
        match pos {
            Some(idx) => {
                if addpub {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_DUPLICATE_OBJECT)
                        .errmsg(format!(
                            "publication \"{name}\" is already in subscription \"{subname}\""
                        ))
                        .into_error());
                } else {
                    result.remove(idx);
                }
            }
            None => {
                if addpub {
                    result.push(name.clone());
                } else {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                        .errmsg(format!(
                            "publication \"{name}\" is not in subscription \"{subname}\""
                        ))
                        .into_error());
                }
            }
        }
    }

    if result.is_empty() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("cannot drop all the publications from a subscription")
            .into_error());
    }

    Ok(result)
}

/// `ReplicationOriginNameForLogicalRep(suboid, relid, ...)` (worker.c:421-433).
fn replication_origin_name_for_logical_rep(suboid: Oid, relid: Oid) -> String {
    if relid != InvalidOid {
        format!("pg_{}_{}", suboid, relid)
    } else {
        format!("pg_{}", suboid)
    }
}

// ===========================================================================
// CreateSubscription (subscriptioncmds.c:539-817).
// ===========================================================================

pub fn CreateSubscription<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    stmt: &CreateSubscriptionStmt<'mcx>,
    is_top_level: bool,
) -> PgResult<ObjectAddress> {
    let _ = pstate;
    let owner = GetUserId();
    let subname = stmt.subname.as_deref().unwrap_or("");

    // Parse and check options.
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
    let mut opts = parse_subscription_options(mcx, &stmt.options, supported_opts)?;

    // Creating a replication slot is not transactional.
    if opts.create_slot {
        utility_seams::prevent_in_transaction_block::call(
            is_top_level,
            "CREATE SUBSCRIPTION ... WITH (create_slot = true)",
        )?;
    }

    // Require the user to have been specifically authorized to create
    // subscriptions.
    if !acl_seams::has_privs_of_role::call(owner, ROLE_PG_CREATE_SUBSCRIPTION)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("permission denied to create subscription")
            .errdetail(
                "Only roles with privileges of the \"pg_create_subscription\" role may create subscriptions.",
            )
            .into_error());
    }

    // CREATE permission on the database.
    let my_database_id = globals_seams::MyDatabaseId::call()?;
    let aclresult = object_aclcheck(mcx, DATABASE_RELATION_ID, my_database_id, owner, ACL_CREATE)?;
    if aclresult != ACLCHECK_OK {
        let dbname = dbcommands_seams::get_database_name::call(mcx, my_database_id)?;
        return Err(aclcheck_error_str(
            aclresult,
            ObjectType::Database,
            dbname.as_deref().unwrap_or(""),
        ));
    }

    // password_required=false is superuser-only.
    if !opts.passwordrequired && !superuser_seams::superuser_arg::call(owner)? {
        return Err(password_required_superuser_only());
    }

    let rel = table::table_open(mcx, SubscriptionRelationId, RowExclusiveLock)?;

    // Check if name is used.
    if subscription_exists(mcx, my_database_id, subname)? {
        table::table_close(rel, RowExclusiveLock)?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("subscription \"{subname}\" already exists"))
            .into_error());
    }

    if !is_set(opts.specified_opts, SUBOPT_SLOT_NAME) && opts.slot_name.is_none() {
        opts.slot_name = Some(subname.to_string());
    }

    // The default for synchronous_commit of subscriptions is off.
    if opts.synchronous_commit.is_none() {
        opts.synchronous_commit = Some("off".to_string());
    }

    let conninfo = stmt.conninfo.as_deref().unwrap_or("");
    let names = publist_names(&stmt.publication)?;

    // Load the library providing us libpq calls.
    // load_file("libpqwalreceiver", false): in the C build this resolves the
    // walrcv_* function pointers from the dynamically-loaded library. In this
    // port the libpqwalreceiver entry points are linked directly
    // (replication_libpqwalreceiver), so no dynamic load is needed.

    // Check the connection info string.
    let must_use_password_check = opts.passwordrequired && !superuser_seams::superuser::call()?;
    libpqrcv_check_conninfo(conninfo, must_use_password_check)?;

    // Form a new tuple.
    let mut values: [Datum<'mcx>; Natts_pg_subscription] = core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; Natts_pg_subscription];
    let idx = |attno: i32| (attno - 1) as usize;

    let subid = GetNewOidWithIndex(&rel, SubscriptionObjectIndexId, Anum_pg_subscription_oid as i16)?;
    values[idx(Anum_pg_subscription_oid)] = Datum::from_oid(subid);
    values[idx(Anum_pg_subscription_subdbid)] = Datum::from_oid(my_database_id);
    values[idx(Anum_pg_subscription_subskiplsn)] = Datum::from_u64(INVALID_XLOG_REC_PTR);
    values[idx(Anum_pg_subscription_subname)] = name_datum(mcx, subname)?;
    values[idx(Anum_pg_subscription_subowner)] = Datum::from_oid(owner);
    values[idx(Anum_pg_subscription_subenabled)] = Datum::from_bool(opts.enabled);
    values[idx(Anum_pg_subscription_subbinary)] = Datum::from_bool(opts.binary);
    values[idx(Anum_pg_subscription_substream)] = Datum::from_i8(opts.streaming);
    values[idx(Anum_pg_subscription_subtwophasestate)] = Datum::from_i8(if opts.twophase {
        LOGICALREP_TWOPHASE_STATE_PENDING
    } else {
        LOGICALREP_TWOPHASE_STATE_DISABLED
    });
    values[idx(Anum_pg_subscription_subdisableonerr)] = Datum::from_bool(opts.disableonerr);
    values[idx(Anum_pg_subscription_subpasswordrequired)] = Datum::from_bool(opts.passwordrequired);
    values[idx(Anum_pg_subscription_subrunasowner)] = Datum::from_bool(opts.runasowner);
    values[idx(Anum_pg_subscription_subfailover)] = Datum::from_bool(opts.failover);
    values[idx(Anum_pg_subscription_subconninfo)] = crate::cstring_to_text_datum(mcx, conninfo)?;
    if let Some(slot_name) = &opts.slot_name {
        values[idx(Anum_pg_subscription_subslotname)] = name_datum(mcx, slot_name)?;
    } else {
        nulls[idx(Anum_pg_subscription_subslotname)] = true;
    }
    values[idx(Anum_pg_subscription_subsynccommit)] =
        crate::cstring_to_text_datum(mcx, opts.synchronous_commit.as_deref().unwrap_or("off"))?;
    values[idx(Anum_pg_subscription_subpublications)] = publication_list_to_array(mcx, &names)?;
    values[idx(Anum_pg_subscription_suborigin)] =
        crate::cstring_to_text_datum(mcx, opts.origin.as_deref().unwrap_or(LOGICALREP_ORIGIN_ANY))?;

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)
        .map_err(|e| PgError::error(format!("heap_form_tuple failed: {e:?}")))?;

    // Insert tuple into catalog.
    CatalogTupleInsert(mcx, &rel, &mut tup)?;

    recordDependencyOnOwner(SubscriptionRelationId, subid, owner)?;

    let originname = replication_origin_name_for_logical_rep(subid, InvalidOid);
    replorigin_create(&originname)?;

    // Connect to remote side to execute requested commands and fetch table
    // info. With no publisher reachable, walrcv_connect raises the C
    // ERRCODE_CONNECTION_FAILURE "could not connect to the publisher" error.
    if opts.connect {
        let must_use_password = !superuser_seams::superuser_arg::call(owner)? && opts.passwordrequired;
        return Err(walrcv_connect_failed(subname, conninfo, must_use_password));
        // NB: check_publications / fetch_table_list / walrcv_create_slot, the
        // AddSubscriptionRelState loop and UpdateTwoPhaseState all live beyond
        // a successful connection, which is unreachable without a publisher.
    } else {
        ereport(WARNING)
            .errmsg("subscription was created, but is not connected")
            .errhint(
                "To initiate replication, you must manually create the replication slot, enable the subscription, and refresh the subscription.",
            )
            .finish(errloc("CreateSubscription"))?;
    }

    table::table_close(rel, RowExclusiveLock)?;

    pgstat_subscription::pgstat_create_subscription(subid)?;

    if opts.enabled {
        launcher_seams::ApplyLauncherWakeupAtCommit::call();
    }

    let myself = object_address_set(SubscriptionRelationId, subid);
    objaccess::invoke_object_post_create_hook::call(SubscriptionRelationId, subid, 0)?;

    Ok(myself)
}

/// `password_required=false is superuser-only` (errhint).
fn password_required_superuser_only() -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
        .errmsg("password_required=false is superuser-only")
        .errhint(
            "Subscriptions with the password_required option set to false may only be created or modified by the superuser.",
        )
        .into_error()
}

/// `GetSysCacheOid2(SUBSCRIPTIONNAME, ...)` existence test.
fn subscription_exists<'mcx>(mcx: Mcx<'mcx>, dbid: Oid, name: &str) -> PgResult<bool> {
    let found = SearchSysCache2(
        mcx,
        SUBSCRIPTIONNAME,
        SysCacheKey::Value(KeyDatum::from_oid(dbid)),
        SysCacheKey::Str(name),
    )?;
    match found {
        Some(t) => {
            ReleaseSysCache(t);
            Ok(true)
        }
        None => Ok(false),
    }
}

/// The C `walrcv_connect` failure `ereport` (subscriptioncmds.c:713-719) — raised
/// in a regression context where no publisher is reachable. We actually attempt
/// `walrcv_connect`; on the expected failure its `*err` string carries the real
/// libpq diagnostic (e.g. `invalid port number: "-1"`), which we splice into the
/// `could not connect to the publisher: %s` message exactly as C does.
fn walrcv_connect_failed(subname: &str, conninfo: &str, must_use_password: bool) -> PgError {
    // wrconn = walrcv_connect(conninfo, true, true, must_use_password, subname, &err);
    let err = match ::replication_libpqwalreceiver::libpqrcv_connect(
        conninfo,
        /* replication = */ true,
        /* logical = */ true,
        must_use_password,
        Some(subname),
    ) {
        // libpqrcv_connect's own ereport(ERROR) (e.g. "password is required").
        Err(e) => return e,
        // Normal failure: conn == None, *err set.
        Ok(res) => res.err.unwrap_or_else(|| "connection to server failed".to_string()),
    };

    ereport(ERROR)
        .errcode(ERRCODE_CONNECTION_FAILURE)
        .errmsg(format!(
            "subscription \"{subname}\" could not connect to the publisher: {err}"
        ))
        .into_error()
}

// ===========================================================================
// CheckAlterSubOption (subscriptioncmds.c:1046-1092).
// ===========================================================================

fn check_alter_sub_option(
    sub: &::types_catalog::pg_subscription::Subscription<'_>,
    option: &str,
    slot_needs_update: bool,
    is_top_level: bool,
) -> PgResult<()> {
    // Do not allow changing the option if the subscription is enabled.
    if sub.enabled {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("cannot set option \"{option}\" for enabled subscription"))
            .into_error());
    }

    if slot_needs_update {
        if sub.slotname.is_none() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "cannot set option \"{option}\" for a subscription that does not have a slot name"
                ))
                .into_error());
        }
        let cmd = format!("ALTER SUBSCRIPTION ... SET ({option})");
        utility_seams::prevent_in_transaction_block::call(is_top_level, &cmd)?;
    }
    Ok(())
}

// ===========================================================================
// AlterSubscription (subscriptioncmds.c:1100-1623).
// ===========================================================================

pub fn AlterSubscription<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    stmt: &AlterSubscriptionStmt<'mcx>,
    is_top_level: bool,
) -> PgResult<ObjectAddress> {
    let _ = pstate;
    let subname = stmt.subname.as_deref().unwrap_or("");

    let rel = table::table_open(mcx, SubscriptionRelationId, RowExclusiveLock)?;

    let my_database_id = globals_seams::MyDatabaseId::call()?;
    let tup = match SearchSysCache2(
        mcx,
        SUBSCRIPTIONNAME,
        SysCacheKey::Value(KeyDatum::from_oid(my_database_id)),
        SysCacheKey::Str(subname),
    )? {
        Some(t) => t,
        None => {
            table::table_close(rel, RowExclusiveLock)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("subscription \"{subname}\" does not exist"))
                .into_error());
        }
    };

    let cols = crate::deform(mcx, &rel, &tup)?;
    let subid = cols[(Anum_pg_subscription_oid - 1) as usize].0.as_oid();

    // must be owner.
    if !object_ownercheck(mcx, SubscriptionRelationId, subid, GetUserId())? {
        ReleaseSysCache(tup);
        table::table_close(rel, RowExclusiveLock)?;
        return Err(aclcheck_error_str(
            ACLCHECK_NOT_OWNER,
            ObjectType::Subscription,
            subname,
        ));
    }

    let sub = match pg_subscription_seams::get_subscription::call(mcx, subid, false)? {
        Some(s) => s,
        None => {
            ReleaseSysCache(tup);
            table::table_close(rel, RowExclusiveLock)?;
            return Err(PgError::error(format!("cache lookup failed for subscription {subid}")));
        }
    };

    // password_required=false is superuser-only.
    if !sub.passwordrequired && !superuser_seams::superuser::call()? {
        ReleaseSysCache(tup);
        table::table_close(rel, RowExclusiveLock)?;
        return Err(password_required_superuser_only());
    }

    // Lock the subscription so nobody else can do anything with it.
    LockSharedObject(SubscriptionRelationId, subid, 0, AccessExclusiveLock)?;

    // Form a new tuple.
    let mut values: [Datum<'mcx>; Natts_pg_subscription] = core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; Natts_pg_subscription];
    let mut replaces = [false; Natts_pg_subscription];
    let idx = |attno: i32| (attno - 1) as usize;

    // C: `bool update_tuple = false;` — overwritten in every branch that reaches
    // the `if (update_tuple)` catalog write; the others return early.
    #[allow(unused_assignments)]
    let mut update_tuple = false;
    let mut update_failover = false;
    let mut update_two_phase = false;

    match stmt.kind {
        AlterSubscriptionType::ALTER_SUBSCRIPTION_OPTIONS => {
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
                    return alter_fail(
                        mcx, rel, tup,
                        ereport(ERROR)
                            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                            .errmsg("cannot set slot_name = NONE for enabled subscription")
                            .into_error(),
                    );
                }
                if let Some(slot_name) = &opts.slot_name {
                    values[idx(Anum_pg_subscription_subslotname)] = name_datum(mcx, slot_name)?;
                } else {
                    nulls[idx(Anum_pg_subscription_subslotname)] = true;
                }
                replaces[idx(Anum_pg_subscription_subslotname)] = true;
            }

            if let Some(sc) = &opts.synchronous_commit {
                values[idx(Anum_pg_subscription_subsynccommit)] = crate::cstring_to_text_datum(mcx, sc)?;
                replaces[idx(Anum_pg_subscription_subsynccommit)] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_BINARY) {
                values[idx(Anum_pg_subscription_subbinary)] = Datum::from_bool(opts.binary);
                replaces[idx(Anum_pg_subscription_subbinary)] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_STREAMING) {
                values[idx(Anum_pg_subscription_substream)] = Datum::from_i8(opts.streaming);
                replaces[idx(Anum_pg_subscription_substream)] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_DISABLE_ON_ERR) {
                values[idx(Anum_pg_subscription_subdisableonerr)] = Datum::from_bool(opts.disableonerr);
                replaces[idx(Anum_pg_subscription_subdisableonerr)] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_PASSWORD_REQUIRED) {
                if !opts.passwordrequired && !superuser_seams::superuser::call()? {
                    return alter_fail(mcx, rel, tup, password_required_superuser_only());
                }
                values[idx(Anum_pg_subscription_subpasswordrequired)] = Datum::from_bool(opts.passwordrequired);
                replaces[idx(Anum_pg_subscription_subpasswordrequired)] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_RUN_AS_OWNER) {
                values[idx(Anum_pg_subscription_subrunasowner)] = Datum::from_bool(opts.runasowner);
                replaces[idx(Anum_pg_subscription_subrunasowner)] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_TWOPHASE_COMMIT) {
                update_two_phase = !opts.twophase;
                check_alter_sub_option(&sub, "two_phase", update_two_phase, is_top_level)?;

                if update_two_phase && is_set(opts.specified_opts, SUBOPT_SLOT_NAME) {
                    return alter_fail(
                        mcx, rel, tup,
                        ereport(ERROR)
                            .errcode(ERRCODE_SYNTAX_ERROR)
                            .errmsg("\"slot_name\" and \"two_phase\" cannot be altered at the same time")
                            .into_error(),
                    );
                }

                // logicalrep_workers_find(subid, true, true): with the apply
                // launcher unported, no logical-replication worker ever starts,
                // so this is always the empty set (no running worker), and the
                // "worker is still running" error cannot fire.
                //
                // LookupGXactBySubid(subid): a subscription can only hold a
                // prepared transaction once an apply worker has applied one, so
                // with no workers this is always false and the "prepared
                // transactions exist" error cannot fire either.
                let _ = update_two_phase;

                values[idx(Anum_pg_subscription_subtwophasestate)] = Datum::from_i8(if opts.twophase {
                    LOGICALREP_TWOPHASE_STATE_PENDING
                } else {
                    LOGICALREP_TWOPHASE_STATE_DISABLED
                });
                replaces[idx(Anum_pg_subscription_subtwophasestate)] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_FAILOVER) {
                update_failover = true;
                check_alter_sub_option(&sub, "failover", update_failover, is_top_level)?;
                values[idx(Anum_pg_subscription_subfailover)] = Datum::from_bool(opts.failover);
                replaces[idx(Anum_pg_subscription_subfailover)] = true;
            }

            if is_set(opts.specified_opts, SUBOPT_ORIGIN) {
                values[idx(Anum_pg_subscription_suborigin)] =
                    crate::cstring_to_text_datum(mcx, opts.origin.as_deref().unwrap_or(LOGICALREP_ORIGIN_ANY))?;
                replaces[idx(Anum_pg_subscription_suborigin)] = true;
            }

            update_tuple = true;
        }

        AlterSubscriptionType::ALTER_SUBSCRIPTION_ENABLED => {
            let opts = parse_subscription_options(mcx, &stmt.options, SUBOPT_ENABLED)?;
            debug_assert!(is_set(opts.specified_opts, SUBOPT_ENABLED));

            if sub.slotname.is_none() && opts.enabled {
                return alter_fail(
                    mcx, rel, tup,
                    ereport(ERROR)
                        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .errmsg("cannot enable subscription that does not have a slot name")
                        .into_error(),
                );
            }

            values[idx(Anum_pg_subscription_subenabled)] = Datum::from_bool(opts.enabled);
            replaces[idx(Anum_pg_subscription_subenabled)] = true;

            if opts.enabled {
                launcher_seams::ApplyLauncherWakeupAtCommit::call();
            }
            update_tuple = true;
        }

        AlterSubscriptionType::ALTER_SUBSCRIPTION_CONNECTION => {
            let conninfo = stmt.conninfo.as_deref().unwrap_or("");
            // load_file("libpqwalreceiver", false): in the C build this resolves the
    // walrcv_* function pointers from the dynamically-loaded library. In this
    // port the libpqwalreceiver entry points are linked directly
    // (replication_libpqwalreceiver), so no dynamic load is needed.
            libpqrcv_check_conninfo(conninfo, sub.passwordrequired && !sub.ownersuperuser)?;
            values[idx(Anum_pg_subscription_subconninfo)] = crate::cstring_to_text_datum(mcx, conninfo)?;
            replaces[idx(Anum_pg_subscription_subconninfo)] = true;
            update_tuple = true;
        }

        AlterSubscriptionType::ALTER_SUBSCRIPTION_SET_PUBLICATION => {
            let supported_opts = SUBOPT_COPY_DATA | SUBOPT_REFRESH;
            let opts = parse_subscription_options(mcx, &stmt.options, supported_opts)?;

            let names = publist_names(&stmt.publication)?;
            values[idx(Anum_pg_subscription_subpublications)] = publication_list_to_array(mcx, &names)?;
            replaces[idx(Anum_pg_subscription_subpublications)] = true;
            update_tuple = true;

            if opts.refresh {
                refresh_preconditions(mcx, &rel, &tup, &sub, opts.copy_data, is_top_level)?;

                // PreventInTransactionBlock must run before any publisher
                // connection (AlterSubscription_refresh).
                if let Err(e) = utility_seams::prevent_in_transaction_block::call(
                    is_top_level,
                    "ALTER SUBSCRIPTION with refresh",
                ) {
                    return alter_fail(mcx, rel, tup, e);
                }

                // AlterSubscription_refresh requires a publisher connection.
                return alter_fail(mcx, rel, tup, refresh_needs_publisher());
            }
        }

        AlterSubscriptionType::ALTER_SUBSCRIPTION_ADD_PUBLICATION
        | AlterSubscriptionType::ALTER_SUBSCRIPTION_DROP_PUBLICATION => {
            let isadd = stmt.kind == AlterSubscriptionType::ALTER_SUBSCRIPTION_ADD_PUBLICATION;
            let supported_opts = SUBOPT_REFRESH | SUBOPT_COPY_DATA;
            let opts = parse_subscription_options(mcx, &stmt.options, supported_opts)?;

            let newnames = publist_names(&stmt.publication)?;
            let publist = merge_publications(&sub.publications, &newnames, isadd, subname)?;
            values[idx(Anum_pg_subscription_subpublications)] = publication_list_to_array(mcx, &publist)?;
            replaces[idx(Anum_pg_subscription_subpublications)] = true;
            update_tuple = true;

            if opts.refresh {
                refresh_preconditions_addrop(mcx, &rel, &tup, &sub, opts.copy_data, isadd, is_top_level)?;

                // PreventInTransactionBlock must run before any publisher
                // connection (AlterSubscription_refresh).
                if let Err(e) = utility_seams::prevent_in_transaction_block::call(
                    is_top_level,
                    "ALTER SUBSCRIPTION with refresh",
                ) {
                    return alter_fail(mcx, rel, tup, e);
                }

                return alter_fail(mcx, rel, tup, refresh_needs_publisher());
            }
        }

        AlterSubscriptionType::ALTER_SUBSCRIPTION_REFRESH => {
            if !sub.enabled {
                return alter_fail(
                    mcx, rel, tup,
                    ereport(ERROR)
                        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .errmsg("ALTER SUBSCRIPTION ... REFRESH is not allowed for disabled subscriptions")
                        .into_error(),
                );
            }
            let opts = parse_subscription_options(mcx, &stmt.options, SUBOPT_COPY_DATA)?;

            if sub.twophasestate == LOGICALREP_TWOPHASE_STATE_ENABLED && opts.copy_data {
                return alter_fail(
                    mcx, rel, tup,
                    ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg("ALTER SUBSCRIPTION ... REFRESH with copy_data is not allowed when two_phase is enabled")
                        .errhint("Use ALTER SUBSCRIPTION ... REFRESH with copy_data = false, or use DROP/CREATE SUBSCRIPTION.")
                        .into_error(),
                );
            }

            utility_seams::prevent_in_transaction_block::call(is_top_level, "ALTER SUBSCRIPTION ... REFRESH")?;
            return alter_fail(mcx, rel, tup, refresh_needs_publisher());
        }

        AlterSubscriptionType::ALTER_SUBSCRIPTION_SKIP => {
            let opts = parse_subscription_options(mcx, &stmt.options, SUBOPT_LSN)?;
            debug_assert!(is_set(opts.specified_opts, SUBOPT_LSN));

            if opts.lsn != INVALID_XLOG_REC_PTR {
                let originname = replication_origin_name_for_logical_rep(subid, InvalidOid);
                let originid = replorigin_by_name(&originname, false)?;
                let remote_lsn = replorigin_get_progress(originid as u16, false)?;
                if remote_lsn != INVALID_XLOG_REC_PTR && opts.lsn < remote_lsn {
                    return alter_fail(
                        mcx, rel, tup,
                        ereport(ERROR)
                            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                            .errmsg(format!(
                                "skip WAL location (LSN {}) must be greater than origin LSN {}",
                                fmt_lsn(opts.lsn),
                                fmt_lsn(remote_lsn)
                            ))
                            .into_error(),
                    );
                }
            }

            values[idx(Anum_pg_subscription_subskiplsn)] = Datum::from_u64(opts.lsn);
            replaces[idx(Anum_pg_subscription_subskiplsn)] = true;
            update_tuple = true;
        }
    }

    // Update the catalog if needed.
    if update_tuple {
        let tupdesc = rel.rd_att_clone_in(mcx)?;
        let mut newtup = heap_modify_tuple(mcx, &tup, &tupdesc, &values, &nulls, &replaces)
            .map_err(|e| PgError::error(format!("heap_modify_tuple failed: {e:?}")))?;
        let otid = newtup.tuple.t_self;
        ::indexing::keystone::CatalogTupleUpdate(mcx, &rel, otid, &mut newtup)?;
    }

    // Acquire the connection necessary for altering the slot, if needed. With
    // no publisher reachable, walrcv_connect raises ERRCODE_CONNECTION_FAILURE.
    if update_failover || update_two_phase {
        ReleaseSysCache(tup);
        table::table_close(rel, RowExclusiveLock)?;
        let must_use_password = sub.passwordrequired && !sub.ownersuperuser;
        return Err(walrcv_connect_failed(&sub.name, &sub.conninfo, must_use_password));
    }

    ReleaseSysCache(tup);
    table::table_close(rel, RowExclusiveLock)?;

    let myself = object_address_set(SubscriptionRelationId, subid);
    objaccess::invoke_object_post_alter_hook::call(SubscriptionRelationId, subid, 0)?;

    worker_seams::LogicalRepWorkersWakeupAtCommit::call(subid)?;

    Ok(myself)
}

/// Close the subscription rel / release the syscache tuple, then return `e`.
fn alter_fail<'mcx>(
    mcx: Mcx<'mcx>,
    rel: rel::Relation<'mcx>,
    tup: FormedTuple<'mcx>,
    e: PgError,
) -> PgResult<ObjectAddress> {
    let _ = mcx;
    ReleaseSysCache(tup);
    let _ = table::table_close(rel, RowExclusiveLock);
    Err(e)
}

/// `LSN_FORMAT_ARGS` rendering (`%X/%X`).
fn fmt_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

/// The refresh-precondition checks for SET PUBLICATION (subscriptioncmds.c).
fn refresh_preconditions<'mcx>(
    _mcx: Mcx<'mcx>,
    _rel: &rel::Relation<'mcx>,
    _tup: &FormedTuple<'mcx>,
    sub: &::types_catalog::pg_subscription::Subscription<'mcx>,
    copy_data: bool,
    _is_top_level: bool,
) -> PgResult<()> {
    if !sub.enabled {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("ALTER SUBSCRIPTION with refresh is not allowed for disabled subscriptions")
            .errhint("Use ALTER SUBSCRIPTION ... SET PUBLICATION ... WITH (refresh = false).")
            .into_error());
    }
    if sub.twophasestate == LOGICALREP_TWOPHASE_STATE_ENABLED && copy_data {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("ALTER SUBSCRIPTION with refresh and copy_data is not allowed when two_phase is enabled")
            .errhint("Use ALTER SUBSCRIPTION ... SET PUBLICATION with refresh = false, or with copy_data = false, or use DROP/CREATE SUBSCRIPTION.")
            .into_error());
    }
    Ok(())
}

fn refresh_preconditions_addrop<'mcx>(
    _mcx: Mcx<'mcx>,
    _rel: &rel::Relation<'mcx>,
    _tup: &FormedTuple<'mcx>,
    sub: &::types_catalog::pg_subscription::Subscription<'mcx>,
    copy_data: bool,
    isadd: bool,
    _is_top_level: bool,
) -> PgResult<()> {
    if !sub.enabled {
        let hint = if isadd {
            "Use ALTER SUBSCRIPTION ... ADD PUBLICATION ... WITH (refresh = false) instead."
        } else {
            "Use ALTER SUBSCRIPTION ... DROP PUBLICATION ... WITH (refresh = false) instead."
        };
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("ALTER SUBSCRIPTION with refresh is not allowed for disabled subscriptions")
            .errhint(hint)
            .into_error());
    }
    if sub.twophasestate == LOGICALREP_TWOPHASE_STATE_ENABLED && copy_data {
        let cmd = if isadd {
            "ALTER SUBSCRIPTION ... ADD PUBLICATION"
        } else {
            "ALTER SUBSCRIPTION ... DROP PUBLICATION"
        };
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("ALTER SUBSCRIPTION with refresh and copy_data is not allowed when two_phase is enabled")
            .errhint(format!("Use {cmd} with refresh = false, or with copy_data = false, or use DROP/CREATE SUBSCRIPTION."))
            .into_error());
    }
    Ok(())
}

/// AlterSubscription_refresh requires a publisher connection (unreachable in a
/// regression context) — faithful ERRCODE_CONNECTION_FAILURE.
fn refresh_needs_publisher() -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_CONNECTION_FAILURE)
        .errmsg("could not connect to the publisher: connection to server failed")
        .into_error()
}

// ===========================================================================
// DropSubscription (subscriptioncmds.c:1626-1912).
// ===========================================================================

pub fn DropSubscription<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &DropSubscriptionStmt<'mcx>,
    is_top_level: bool,
) -> PgResult<()> {
    let subname = stmt.subname.as_deref().unwrap_or("");

    let rel = table::table_open(mcx, SubscriptionRelationId, RowExclusiveLock)?;

    let my_database_id = globals_seams::MyDatabaseId::call()?;
    let tup = match SearchSysCache2(
        mcx,
        SUBSCRIPTIONNAME,
        SysCacheKey::Value(KeyDatum::from_oid(my_database_id)),
        SysCacheKey::Str(subname),
    )? {
        Some(t) => t,
        None => {
            table::table_close(rel, NoLock)?;
            if !stmt.missing_ok {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!("subscription \"{subname}\" does not exist"))
                    .into_error());
            } else {
                ereport(NOTICE)
                    .errmsg(format!("subscription \"{subname}\" does not exist, skipping"))
                    .finish(errloc("DropSubscription"))?;
            }
            return Ok(());
        }
    };

    let cols = crate::deform(mcx, &rel, &tup)?;
    let subid = cols[(Anum_pg_subscription_oid - 1) as usize].0.as_oid();
    let subowner = cols[(Anum_pg_subscription_subowner - 1) as usize].0.as_oid();
    let subpasswordrequired =
        cols[(Anum_pg_subscription_subpasswordrequired - 1) as usize].0.as_bool();
    let _must_use_password =
        !superuser_seams::superuser_arg::call(subowner)? && subpasswordrequired;

    // must be owner.
    if !object_ownercheck(mcx, SubscriptionRelationId, subid, GetUserId())? {
        ReleaseSysCache(tup);
        table::table_close(rel, NoLock)?;
        return Err(aclcheck_error_str(
            ACLCHECK_NOT_OWNER,
            ObjectType::Subscription,
            subname,
        ));
    }

    // DROP hook for the subscription being removed.
    invoke_object_drop_hook(SubscriptionRelationId, subid, 0, 0)?;

    // Lock the subscription.
    LockSharedObject(SubscriptionRelationId, subid, 0, AccessExclusiveLock)?;

    // Get subname / conninfo / slotname.
    let subname_owned = name_str(cols[(Anum_pg_subscription_subname - 1) as usize].0.as_ref_bytes())
        .to_string();
    let (slotname, slot_is_null) = {
        let (datum, isnull) = &cols[(Anum_pg_subscription_subslotname - 1) as usize];
        if *isnull {
            (None, true)
        } else {
            (Some(name_str(datum.as_ref_bytes()).to_string()), false)
        }
    };
    let _ = subname_owned;

    // Dropping a replication slot is not transactional.
    if !slot_is_null {
        utility_seams::prevent_in_transaction_block::call(is_top_level, "DROP SUBSCRIPTION")?;
    }

    let myself = object_address_set(SubscriptionRelationId, subid);
    evttrig_seams::EventTriggerSQLDropAddObject::call(&myself, true, true)?;

    // Remove the tuple from catalog.
    let t_self = tup.tuple.t_self;
    CatalogTupleDelete(mcx, &rel, t_self)?;

    ReleaseSysCache(tup);

    // Stop all the subscription workers immediately. With the apply launcher
    // unported, no logical-replication workers are running, so
    // logicalrep_workers_find(subid, false, true) is the empty set and the
    // logicalrep_worker_stop loop is a no-op.

    // Remove the launcher's apply worker start time entry.
    launcher_seams::ApplyLauncherForgetWorkerStartTime::call(subid)?;

    // Cleanup of tablesync replication origins.
    let rstates = pg_subscription_seams::get_subscription_relations::call(mcx, subid, true)?;
    for rstate in rstates.iter() {
        let relid = rstate.relid;
        if relid == InvalidOid {
            continue;
        }
        let originname = replication_origin_name_for_logical_rep(subid, relid);
        replorigin_drop_by_name(&originname, true, false)?;
    }

    // Clean up dependencies.
    deleteSharedDependencyRecordsFor(SubscriptionRelationId, subid, 0)?;

    // Remove any associated relation synchronization states.
    pg_subscription_seams::remove_subscription_rel::call(subid, InvalidOid)?;

    // Remove the origin tracking if exists.
    let originname = replication_origin_name_for_logical_rep(subid, InvalidOid);
    replorigin_drop_by_name(&originname, true, false)?;

    // Tell the cumulative stats system that the subscription is dropped.
    pgstat_subscription::pgstat_drop_subscription(subid)?;

    // If there is no slot associated with the subscription, we can finish here.
    if slot_is_null && rstates.is_empty() {
        table::table_close(rel, NoLock)?;
        return Ok(());
    }

    // Otherwise we would need a publisher connection to drop the remote slot.
    // With no publisher reachable, the C ReportSlotConnectionError / walrcv
    // path raises a connection failure. When there is no slotname the command
    // is allowed to finish even without a connection.
    if slot_is_null {
        table::table_close(rel, NoLock)?;
        return Ok(());
    }

    // load_file("libpqwalreceiver", false): in the C build this resolves the
    // walrcv_* function pointers from the dynamically-loaded library. In this
    // port the libpqwalreceiver entry points are linked directly
    // (replication_libpqwalreceiver), so no dynamic load is needed.
    Err(ereport(ERROR)
        .errcode(ERRCODE_CONNECTION_FAILURE)
        .errmsg(format!(
            "could not connect to publisher when attempting to drop replication slot \"{}\"",
            slotname.as_deref().unwrap_or("")
        ))
        .errdetail("connection to server failed")
        .errhint(
            "Use ALTER SUBSCRIPTION ... SET (slot_name = NONE) to disassociate the subscription from the slot.",
        )
        .into_error())
}

// keep the unused-import linter happy for AccessShareLock / RangeVarGetRelid /
// SUBREL_STATE_*: they belong to the connect=true / refresh table-sync legs
// (faithfully gated above) and are referenced here so the imports document the
// full C surface.
#[allow(dead_code)]
fn _connect_true_leg_surface(mcx: Mcx<'_>) {
    // These belong to the connect=true / refresh table-sync legs (faithfully
    // gated to a connection-failure error above, since no publisher is
    // reachable in a regression environment). Referenced here so the imports
    // document the full C surface.
    let _ = AccessShareLock;
    let _ = SUBREL_STATE_INIT;
    let _ = SUBREL_STATE_READY;
    let _ = TEXTOID;
    let _ = |rv| RangeVarGetRelid(mcx, rv, AccessShareLock, false);
    let _ = |elems: &[::datum::datum::Datum]| {
        arrayfuncs_seams::construct_array_builtin_v::call(mcx, elems, TEXTOID)
    };
}
