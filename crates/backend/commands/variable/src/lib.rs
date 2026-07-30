#![allow(non_snake_case)]

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};

use adt_datetime::consts::{
    DATEORDER_DMY, DATEORDER_MDY, DATEORDER_YMD, USE_GERMAN_DATES, USE_ISO_DATES,
    USE_POSTGRES_DATES, USE_SQL_DATES,
};
use adt_datetime::tz::{self, PgTz};
use adt_datetime::ClearTimeZoneAbbrevCache;
use elog::{elog, ereport};
use guc::{GUC_check_errcode, GUC_check_errdetail, GUC_check_errhint, GUC_check_errmsg};
use guc_tables::{hooks, vars, GucHookExtra, GucVarAccessors};
use types_core::{BackendType, InvalidOid, Oid, XACT_SERIALIZABLE};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_ACTIVE_SQL_TRANSACTION, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_TRANSACTION_STATE,
    ERRCODE_UNDEFINED_OBJECT, LOG, NOTICE,
};
use types_guc::{GucSource, PGC_S_DEFAULT, PGC_S_INTERACTIVE, PGC_S_TEST};

#[cfg(test)]
mod tests;

const SECS_PER_HOUR: i64 = 3600;

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("commands/variable.c hook arm not ported: {what}");
}

fn initializing_parallel_worker() -> bool {
    parallel_seams::initializing_parallel_worker::is_installed()
        && parallel_seams::initializing_parallel_worker::call()
}

fn is_parallel_worker() -> bool {
    parallel_seams::is_parallel_worker::is_installed()
        && parallel_seams::is_parallel_worker::call()
}

fn parse_datestyle(
    value: &str,
    source: GucSource,
    style: &mut i32,
    order: &mut i32,
    have_style: &mut bool,
    have_order: &mut bool,
) -> PgResult<bool> {
    // Cold SET/boot path: per-call scratch context mirrors C's palloc churn.
    let ctx = mcx::MemoryContext::new("check_datestyle");
    let Some(elemlist) = varlena::split_identifier_string(
        ctx.mcx(),
        value,
        b',',
        mbutils::GetDatabaseEncoding(),
    )?
    else {
        GUC_check_errdetail("List syntax is invalid.");
        return Ok(false);
    };

    let mut ok = true;
    for tok in &elemlist {
        let t = tok.as_str();
        if t.eq_ignore_ascii_case("ISO") {
            if *have_style && *style != USE_ISO_DATES {
                ok = false;
            }
            *style = USE_ISO_DATES;
            *have_style = true;
        } else if t.eq_ignore_ascii_case("SQL") {
            if *have_style && *style != USE_SQL_DATES {
                ok = false;
            }
            *style = USE_SQL_DATES;
            *have_style = true;
        } else if t.len() >= 8 && t.as_bytes()[..8].eq_ignore_ascii_case(b"POSTGRES") {
            if *have_style && *style != USE_POSTGRES_DATES {
                ok = false;
            }
            *style = USE_POSTGRES_DATES;
            *have_style = true;
        } else if t.eq_ignore_ascii_case("GERMAN") {
            if *have_style && *style != USE_GERMAN_DATES {
                ok = false;
            }
            *style = USE_GERMAN_DATES;
            *have_style = true;
            if !*have_order {
                *order = DATEORDER_DMY;
            }
        } else if t.eq_ignore_ascii_case("YMD") {
            if *have_order && *order != DATEORDER_YMD {
                ok = false;
            }
            *order = DATEORDER_YMD;
            *have_order = true;
        } else if t.eq_ignore_ascii_case("DMY")
            || (t.len() >= 4 && t.as_bytes()[..4].eq_ignore_ascii_case(b"EURO"))
        {
            if *have_order && *order != DATEORDER_DMY {
                ok = false;
            }
            *order = DATEORDER_DMY;
            *have_order = true;
        } else if t.eq_ignore_ascii_case("MDY")
            || t.eq_ignore_ascii_case("US")
            || (t.len() >= 7 && t.as_bytes()[..7].eq_ignore_ascii_case(b"NONEURO"))
        {
            if *have_order && *order != DATEORDER_MDY {
                ok = false;
            }
            *order = DATEORDER_MDY;
            *have_order = true;
        } else if t.eq_ignore_ascii_case("DEFAULT") {
            let Some(subval) = guc::GetConfigOptionResetString("datestyle") else {
                ok = false;
                break;
            };
            let mut sub_style = *style;
            let mut sub_order = *order;
            let mut sub_have_style = false;
            let mut sub_have_order = false;
            if !parse_datestyle(
                &subval,
                source,
                &mut sub_style,
                &mut sub_order,
                &mut sub_have_style,
                &mut sub_have_order,
            )? {
                ok = false;
                break;
            }
            if !*have_style {
                *style = sub_style;
            }
            if !*have_order {
                *order = sub_order;
            }
        } else {
            GUC_check_errdetail(format!("Unrecognized key word: \"{t}\"."));
            return Ok(false);
        }
    }

    if !ok {
        GUC_check_errdetail("Conflicting \"DateStyle\" specifications.");
        return Ok(false);
    }
    Ok(true)
}

pub fn check_datestyle(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    source: GucSource,
) -> PgResult<bool> {
    let value = newval.clone().expect("DateStyle GUC value is NULL");
    let mut style = adt_datetime::settings::date_style();
    let mut order = adt_datetime::settings::date_order();
    let mut have_style = false;
    let mut have_order = false;
    if !parse_datestyle(&value, source, &mut style, &mut order, &mut have_style, &mut have_order)? {
        return Ok(false);
    }

    let style_str = match style {
        USE_SQL_DATES => "SQL",
        USE_GERMAN_DATES => "German",
        USE_ISO_DATES => "ISO",
        _ => "Postgres",
    };
    let order_str = match order {
        DATEORDER_YMD => "YMD",
        DATEORDER_DMY => "DMY",
        _ => "MDY",
    };
    *newval = Some(format!("{style_str}, {order_str}"));
    *extra = Some(Box::new((style, order)));
    Ok(true)
}

pub fn assign_datestyle(_newval: Option<&str>, extra: Option<&GucHookExtra>) {
    let &(style, order) = extra
        .and_then(|e| e.downcast_ref::<(i32, i32)>())
        .expect("assign_datestyle extra");
    adt_datetime::settings::set_date_style(style);
    adt_datetime::settings::set_date_order(order);
}

fn check_timezone_value(value: &str, log_only: bool) -> PgResult<Option<&'static PgTz>> {
    if !log_only {
        if value.len() >= 8 && value.as_bytes()[..8].eq_ignore_ascii_case(b"interval") {
            // INTERVAL 'foo': SQL spec compliance arm.
            let rest = value[8..].trim_start_matches([' ', '\t', '\n', '\x0b', '\x0c', '\r']);
            let Some(rest) = rest.strip_prefix('\'') else {
                return Ok(None);
            };
            let Some(end) = rest.find('\'') else {
                return Ok(None);
            };
            if end + 1 != rest.len() {
                return Ok(None);
            }
            let interval = adt_timestamp::interval::interval_in(&rest[..end], -1, None)?;
            if interval.month != 0 {
                GUC_check_errdetail("Cannot specify months in time zone interval.");
                return Ok(None);
            }
            if interval.day != 0 {
                GUC_check_errdetail("Cannot specify days in time zone interval.");
                return Ok(None);
            }
            // SQL to Unix sign convention
            let gmtoffset = -(interval.time / 1_000_000);
            let new_tz = tz::pg_tzset_offset(gmtoffset);
            if new_tz.is_none() {
                GUC_check_errdetail("UTC timezone offset is out of range.");
            }
            return Ok(new_tz);
        }
        if let Ok(hours) = value.parse::<f64>() {
            let gmtoffset = (-hours * SECS_PER_HOUR as f64) as i64;
            let new_tz = tz::pg_tzset_offset(gmtoffset);
            if new_tz.is_none() {
                GUC_check_errdetail("UTC timezone offset is out of range.");
            }
            return Ok(new_tz);
        }
    }
    let Some(new_tz) = tz::pg_tzset(value.as_bytes()) else {
        return Ok(None);
    };
    if !tz::pg_tz_acceptable(new_tz) {
        GUC_check_errmsg(format!("time zone \"{value}\" appears to use leap seconds"));
        GUC_check_errdetail("PostgreSQL does not support leap seconds.");
        return Ok(None);
    }
    Ok(Some(new_tz))
}

pub fn check_timezone(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let value = newval.as_deref().expect("TimeZone GUC value is NULL");
    let Some(new_tz) = check_timezone_value(value, false)? else {
        return Ok(false);
    };
    *extra = Some(Box::new(new_tz));
    Ok(true)
}

pub fn assign_timezone(_newval: Option<&str>, extra: Option<&GucHookExtra>) {
    let tzp = *extra
        .and_then(|e| e.downcast_ref::<&'static PgTz>())
        .expect("assign_timezone extra");
    tz::set_session_timezone(Some(tzp));
    ClearTimeZoneAbbrevCache();
}

pub fn show_timezone() -> String {
    tz::session_timezone()
        .and_then(tz::pg_get_timezone_name)
        .unwrap_or("unknown")
        .to_string()
}

pub fn check_log_timezone(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let value = newval.as_deref().expect("log_timezone GUC value is NULL");
    let Some(new_tz) = check_timezone_value(value, true)? else {
        return Ok(false);
    };
    *extra = Some(Box::new(new_tz));
    Ok(true)
}

pub fn assign_log_timezone(_newval: Option<&str>, extra: Option<&GucHookExtra>) {
    let tzp = *extra
        .and_then(|e| e.downcast_ref::<&'static PgTz>())
        .expect("assign_log_timezone extra");
    tz::set_log_timezone(Some(tzp));
}

pub fn show_log_timezone() -> String {
    tz::log_timezone()
        .and_then(tz::pg_get_timezone_name)
        .unwrap_or("unknown")
        .to_string()
}

pub fn check_timezone_abbreviations(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    source: GucSource,
) -> PgResult<bool> {
    // The boot_val is NULL; pg_timezone_abbrev_initialize supplies "Default"
    // later, so nothing is loaded during InitializeGUCOptions.
    let Some(value) = newval.as_deref() else {
        debug_assert_eq!(source, PGC_S_DEFAULT);
        return Ok(true);
    };
    match tzparser::load_tzoffsets(value) {
        Some(tbl) => {
            *extra = Some(Box::new(tbl));
            Ok(true)
        }
        None => Ok(false),
    }
}

pub fn assign_timezone_abbreviations(_newval: Option<&str>, extra: Option<&GucHookExtra>) {
    let Some(extra) = extra else {
        return;
    };
    let tbl = extra
        .downcast_ref::<&'static tz::ZoneAbbrevTable>()
        .expect("assign_timezone_abbreviations extra");
    tz::InstallTimeZoneAbbrevs(tbl);
}

pub fn check_transaction_read_only(
    newval: &mut bool,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    if !*newval
        && xact::XactReadOnly()
        && xact::IsTransactionState()
        && !initializing_parallel_worker()
    {
        if xact::IsSubTransaction() {
            GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
            GUC_check_errmsg(
                "cannot set transaction read-write mode inside a read-only transaction",
            );
            return Ok(false);
        }
        if snapmgr::FirstSnapshotSet() {
            GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
            GUC_check_errmsg("transaction read-write mode must be set before any query");
            return Ok(false);
        }
        if transam_xlog::RecoveryInProgress() {
            GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
            GUC_check_errmsg("cannot set transaction read-write mode during recovery");
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn check_transaction_isolation(
    newval: &mut i32,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let new_level = *newval;
    if new_level != xact::XactIsoLevel()
        && xact::IsTransactionState()
        && !initializing_parallel_worker()
    {
        if snapmgr::FirstSnapshotSet() {
            GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
            GUC_check_errmsg("SET TRANSACTION ISOLATION LEVEL must be called before any query");
            return Ok(false);
        }
        if xact::IsSubTransaction() {
            GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
            GUC_check_errmsg(
                "SET TRANSACTION ISOLATION LEVEL must not be called in a subtransaction",
            );
            return Ok(false);
        }
        if new_level == XACT_SERIALIZABLE && transam_xlog::RecoveryInProgress() {
            GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
            GUC_check_errmsg("cannot use serializable mode in a hot standby");
            GUC_check_errhint("You can use REPEATABLE READ instead.");
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn check_transaction_deferrable(
    _newval: &mut bool,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    if initializing_parallel_worker() {
        return Ok(true);
    }
    if xact::IsSubTransaction() {
        GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
        GUC_check_errmsg(
            "SET TRANSACTION [NOT] DEFERRABLE cannot be called within a subtransaction",
        );
        return Ok(false);
    }
    if snapmgr::FirstSnapshotSet() {
        GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
        GUC_check_errmsg("SET TRANSACTION [NOT] DEFERRABLE must be called before any query");
        return Ok(false);
    }
    Ok(true)
}

pub fn check_random_seed(
    _newval: &mut f64,
    extra: &mut Option<GucHookExtra>,
    source: GucSource,
) -> PgResult<bool> {
    *extra = Some(Box::new(AtomicBool::new(source >= PGC_S_INTERACTIVE)));
    Ok(true)
}

pub fn assign_random_seed(newval: f64, extra: Option<&GucHookExtra>) {
    let armed = extra
        .and_then(|e| e.downcast_ref::<AtomicBool>())
        .expect("assign_random_seed extra");
    if armed.swap(false, Ordering::Relaxed) {
        // GUC bounds pin newval to [-1,1]; setseed cannot fail on it.
        pseudorandomfuncs::setseed(newval).expect("assign_random_seed: setseed");
    }
}

pub fn show_random_seed() -> String {
    "unavailable".to_string()
}

pub fn check_client_encoding(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let value = newval.clone().expect("client_encoding GUC value is NULL");
    let encoding = mbutils::pg_valid_client_encoding(&value);
    if encoding < 0 {
        return Ok(false);
    }
    let canonical_name = mbutils::pg_encoding_to_char(encoding);

    // Workers send data to the leader in the database encoding; accept the
    // leader's setting during startup, reject any later change.
    if is_parallel_worker() && !initializing_parallel_worker() {
        GUC_check_errcode(ERRCODE_INVALID_TRANSACTION_STATE);
        GUC_check_errdetail("Cannot change \"client_encoding\" during a parallel operation.");
        return Ok(false);
    }

    if !is_parallel_worker() && mbutils::PrepareClientEncoding(encoding)? < 0 {
        if xact::IsTransactionState() {
            GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
            GUC_check_errdetail(format!(
                "Conversion between {} and {} is not supported.",
                canonical_name,
                mbutils::GetDatabaseEncodingName()
            ));
        } else {
            GUC_check_errdetail("Cannot change \"client_encoding\" now.");
        }
        return Ok(false);
    }

    // Keep the pre-9.1 JDBC "UNICODE" spelling un-canonicalized (C's kluge).
    if value != canonical_name && value != "UNICODE" {
        *newval = Some(canonical_name.to_string());
    }
    *extra = Some(Box::new(encoding));
    Ok(true)
}

pub fn assign_client_encoding(_newval: Option<&str>, extra: Option<&GucHookExtra>) {
    let &encoding = extra
        .and_then(|e| e.downcast_ref::<i32>())
        .expect("assign_client_encoding extra");
    if is_parallel_worker() {
        return;
    }
    match mbutils::SetClientEncoding(encoding) {
        Ok(rc) if rc < 0 => {
            elog(LOG, format!("SetClientEncoding({encoding}) failed")).unwrap();
        }
        Ok(_) => {}
        Err(e) => panic!("SetClientEncoding({encoding}): {e:?}"),
    }
}

struct RoleAuthExtra {
    roleid: Oid,
    is_superuser: bool,
}

pub fn check_session_authorization(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    source: GucSource,
) -> PgResult<bool> {
    let Some(value) = newval.clone() else {
        return Ok(true);
    };

    if initializing_parallel_worker() {
        // Copy the leader's state even if it no longer matches the catalogs;
        // ParallelWorkerMain already installed the right OID + superuser bit.
        *extra = Some(Box::new(RoleAuthExtra {
            roleid: miscinit::GetSessionUserId(),
            is_superuser: miscinit::GetSessionUserIsSuperuser(),
        }));
        return Ok(true);
    }

    if !xact::IsTransactionState() {
        return Ok(false);
    }

    let Some((roleid, is_superuser)) = syscache_seams::lookup_authid_by_rolname::call(&value)?
    else {
        if source == PGC_S_TEST {
            ereport(NOTICE)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("role \"{value}\" does not exist"))
                .finish(loc("check_session_authorization"))?;
            return Ok(true);
        }
        GUC_check_errmsg(format!("role \"{value}\" does not exist"));
        return Ok(false);
    };

    if roleid != miscinit::GetAuthenticatedUserId()
        && !superuser::superuser_arg(miscinit::GetAuthenticatedUserId())?
    {
        if source == PGC_S_TEST {
            ereport(NOTICE)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!(
                    "permission will be denied to set session authorization \"{value}\""
                ))
                .finish(loc("check_session_authorization"))?;
            return Ok(true);
        }
        GUC_check_errcode(ERRCODE_INSUFFICIENT_PRIVILEGE);
        GUC_check_errmsg(format!(
            "permission denied to set session authorization \"{value}\""
        ));
        return Ok(false);
    }

    *extra = Some(Box::new(RoleAuthExtra { roleid, is_superuser }));
    Ok(true)
}

pub fn assign_session_authorization(_newval: Option<&str>, extra: Option<&GucHookExtra>) {
    let Some(myextra) = extra.and_then(|e| e.downcast_ref::<RoleAuthExtra>()) else {
        return;
    };
    miscinit::SetSessionAuthorization(myextra.roleid, myextra.is_superuser)
        .expect("SetSessionAuthorization");
}

pub fn check_role(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    source: GucSource,
) -> PgResult<bool> {
    let value = newval.clone().expect("role GUC value is NULL");
    let (roleid, is_superuser);

    if value == "none" {
        roleid = InvalidOid;
        is_superuser = false;
    } else if initializing_parallel_worker() {
        // Copy the leader's state even if it no longer matches the catalogs.
        roleid = miscinit::GetCurrentRoleId();
        is_superuser = guc_tables::vars::current_role_is_superuser.read();
    } else {
        if !xact::IsTransactionState() {
            return Ok(false);
        }

        let Some((oid, rolsuper)) = syscache_seams::lookup_authid_by_rolname::call(&value)? else {
            if source == PGC_S_TEST {
                ereport(NOTICE)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!("role \"{value}\" does not exist"))
                    .finish(loc("check_role"))?;
                return Ok(true);
            }
            GUC_check_errmsg(format!("role \"{value}\" does not exist"));
            return Ok(false);
        };
        roleid = oid;
        is_superuser = rolsuper;

        if !adt_acl::member_can_set_role(miscinit::GetSessionUserId(), roleid)? {
            if source == PGC_S_TEST {
                ereport(NOTICE)
                    .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .errmsg(format!("permission will be denied to set role \"{value}\""))
                    .finish(loc("check_role"))?;
                return Ok(true);
            }
            GUC_check_errcode(ERRCODE_INSUFFICIENT_PRIVILEGE);
            GUC_check_errmsg(format!("permission denied to set role \"{value}\""));
            return Ok(false);
        }
    }

    *extra = Some(Box::new(RoleAuthExtra { roleid, is_superuser }));
    Ok(true)
}

pub fn assign_role(_newval: Option<&str>, extra: Option<&GucHookExtra>) {
    let myextra = extra
        .and_then(|e| e.downcast_ref::<RoleAuthExtra>())
        .expect("assign_role extra");
    miscinit::SetCurrentRoleId(myextra.roleid, myextra.is_superuser).expect("SetCurrentRoleId");
}

pub fn show_role() -> String {
    if miscinit::GetCurrentRoleId() == InvalidOid {
        return "none".to_string();
    }
    vars::role_string.read().unwrap_or_else(|| "none".to_string())
}

// canonicalize_path (port/path.c, Unix arms), inlined until the port unit lands.
pub fn canonicalize_path(path: &str) -> String {
    let absolute = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for comp in path.split('/') {
        match comp {
            "" | "." => {}
            ".." => {
                if let Some(last) = parts.last() {
                    if *last != ".." {
                        parts.pop();
                        continue;
                    }
                }
                if !absolute {
                    parts.push("..");
                }
            }
            c => parts.push(c),
        }
    }
    let body = parts.join("/");
    match (absolute, body.is_empty()) {
        (true, true) => "/".to_string(),
        (true, false) => format!("/{body}"),
        (false, true) => ".".to_string(),
        (false, false) => body,
    }
}

pub fn check_canonical_path(
    newval: &mut Option<String>,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    if let Some(val) = newval.as_deref() {
        *newval = Some(canonicalize_path(val));
    }
    Ok(true)
}

fn pg_clean_ascii(s: &str) -> String {
    pg_string::pg_clean_ascii(s, 0).expect("pg_clean_ascii with alloc_flags=0 cannot fail")
}

pub fn check_application_name(
    newval: &mut Option<String>,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let value = newval.clone().expect("application_name GUC value is NULL");
    *newval = Some(pg_clean_ascii(&value));
    Ok(true)
}

pub fn assign_application_name(newval: Option<&str>, _extra: Option<&GucHookExtra>) {
    backend_status::pgstat_report_appname(newval.unwrap_or(""));
}

pub fn check_cluster_name(
    newval: &mut Option<String>,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let value = newval.clone().expect("cluster_name GUC value is NULL");
    *newval = Some(pg_clean_ascii(&value));
    Ok(true)
}

thread_local! {
    // C homes these in bufmgr.c/aio; local until those units own their globals.
    static MAINTENANCE_IO_CONCURRENCY: Cell<i32> = const { Cell::new(10) };
    static IO_COMBINE_LIMIT_GUC: Cell<i32> = const { Cell::new(16) };
    static IO_MAX_COMBINE_LIMIT: Cell<i32> = const { Cell::new(16) };
    static IO_COMBINE_LIMIT: Cell<i32> = const { Cell::new(16) };
}

pub fn maintenance_io_concurrency() -> i32 {
    MAINTENANCE_IO_CONCURRENCY.with(Cell::get)
}

pub fn io_combine_limit() -> i32 {
    IO_COMBINE_LIMIT.with(Cell::get)
}

pub fn assign_maintenance_io_concurrency(newval: i32, _extra: Option<&GucHookExtra>) {
    MAINTENANCE_IO_CONCURRENCY.with(|c| c.set(newval));
    if miscinit::GetMyBackendType() == BackendType::Startup {
        xlogprefetcher_seams::xlog_prefetch_reconfigure::call();
    }
}

pub fn assign_io_max_combine_limit(newval: i32, _extra: Option<&GucHookExtra>) {
    IO_MAX_COMBINE_LIMIT.with(|c| c.set(newval));
    IO_COMBINE_LIMIT.with(|c| c.set(newval.min(IO_COMBINE_LIMIT_GUC.with(Cell::get))));
}

pub fn assign_io_combine_limit(newval: i32, _extra: Option<&GucHookExtra>) {
    IO_COMBINE_LIMIT_GUC.with(|c| c.set(newval));
    IO_COMBINE_LIMIT.with(|c| c.set(IO_MAX_COMBINE_LIMIT.with(Cell::get).min(newval)));
}

pub fn show_data_directory_mode() -> String {
    // Read the fd file_perm global that checkDataDir derives the mode from —
    // the same source the server's own file creation uses (C reads the
    // data_directory_mode global set from pg_dir_create_mode at the same
    // spot; pg_basebackup clients apply this to everything they create).
    format!("{:04o}", fd::vfd::pg_dir_create_mode())
}

// show_data_checksums (variable.c has none; C wires the GUC via
// SetConfigOption in ReadControlFile — here a show hook over the same
// ControlFile->data_checksum_version predicate).
pub fn show_data_checksums() -> String {
    if transam_xlog_seams::data_checksums_enabled::is_installed()
        && transam_xlog_seams::data_checksums_enabled::call()
    {
        "on".to_string()
    } else {
        "off".to_string()
    }
}

pub fn show_log_file_mode() -> String {
    format!("{:04o}", guc::store::get_int("log_file_mode").unwrap_or(0))
}

pub fn show_unix_socket_permissions() -> String {
    format!(
        "{:04o}",
        guc::store::get_int("unix_socket_permissions").unwrap_or(0)
    )
}

pub fn check_bonjour(
    newval: &mut bool,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    // Build without USE_BONJOUR.
    if *newval {
        GUC_check_errmsg("Bonjour is not supported by this build");
        return Ok(false);
    }
    Ok(true)
}

pub fn check_default_with_oids(
    newval: &mut bool,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    if *newval {
        GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
        GUC_check_errmsg("tables declared WITH OIDS are not supported");
        return Ok(false);
    }
    Ok(true)
}

pub fn check_ssl(
    _newval: &mut bool,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    // USE_SSL build (be_secure_openssl).
    Ok(true)
}

pub fn init_seams() {
    hooks::check_datestyle.install(check_datestyle);
    hooks::assign_datestyle.install(assign_datestyle);
    hooks::check_timezone.install(check_timezone);
    hooks::assign_timezone.install(assign_timezone);
    hooks::show_timezone.install(show_timezone);
    hooks::check_log_timezone.install(check_log_timezone);
    hooks::assign_log_timezone.install(assign_log_timezone);
    hooks::show_log_timezone.install(show_log_timezone);
    hooks::check_timezone_abbreviations.install(check_timezone_abbreviations);
    hooks::assign_timezone_abbreviations.install(assign_timezone_abbreviations);
    hooks::check_transaction_read_only.install(check_transaction_read_only);
    hooks::check_transaction_isolation.install(check_transaction_isolation);
    hooks::check_transaction_deferrable.install(check_transaction_deferrable);
    hooks::check_random_seed.install(check_random_seed);
    hooks::assign_random_seed.install(assign_random_seed);
    hooks::show_random_seed.install(show_random_seed);
    hooks::check_client_encoding.install(check_client_encoding);
    hooks::assign_client_encoding.install(assign_client_encoding);
    hooks::check_session_authorization.install(check_session_authorization);
    hooks::assign_session_authorization.install(assign_session_authorization);
    hooks::check_role.install(check_role);
    hooks::assign_role.install(assign_role);
    hooks::show_role.install(show_role);
    hooks::check_canonical_path.install(check_canonical_path);
    hooks::check_application_name.install(check_application_name);
    hooks::assign_application_name.install(assign_application_name);
    hooks::check_cluster_name.install(check_cluster_name);
    hooks::assign_maintenance_io_concurrency.install(assign_maintenance_io_concurrency);
    hooks::assign_io_max_combine_limit.install(assign_io_max_combine_limit);
    hooks::assign_io_combine_limit.install(assign_io_combine_limit);
    hooks::show_data_directory_mode.install(show_data_directory_mode);
    hooks::show_data_checksums.install(show_data_checksums);
    hooks::show_log_file_mode.install(show_log_file_mode);
    hooks::show_unix_socket_permissions.install(show_unix_socket_permissions);
    hooks::check_bonjour.install(check_bonjour);
    hooks::check_default_with_oids.install(check_default_with_oids);
    hooks::check_ssl.install(check_ssl);

    vars::maintenance_io_concurrency.install(GucVarAccessors {
        get: maintenance_io_concurrency,
        set: |v| MAINTENANCE_IO_CONCURRENCY.with(|c| c.set(v)),
    });
    // io_combine_limit_guc's backing is bufmgr's (C defines it in bufmgr.c);
    // installing it here too paniced every boot ("installed twice").
    vars::io_max_combine_limit.install(GucVarAccessors {
        get: || IO_MAX_COMBINE_LIMIT.with(Cell::get),
        set: |v| IO_MAX_COMBINE_LIMIT.with(|c| c.set(v)),
    });
}
