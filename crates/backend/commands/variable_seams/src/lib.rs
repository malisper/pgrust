//! Seam declarations the `backend-commands-variable` unit
//! (`commands/variable.c`) reaches across to other subsystems through.
//!
//! variable.c is almost entirely cross-subsystem glue: its GUC `check_`/
//! `assign_`/`show_` hooks read and write datetime globals, the timezone
//! library, the GUC framework, encoding tables, roles/auth, transaction state,
//! and pgstat. Each such call is a seam declared here and installed by
//! `variable::init_seams()`, which delegates to the real owner
//! crate (when it is ported) or loud-panics (mirror-pg-and-panic) until the
//! owner lands.
//!
//! Two deliberate adaptations vs the raw C contract:
//!
//! * **No ambient `Mcx`.** GUC hook function pointers carry no memory context
//!   (`guc-tables/slots.rs`). Calls that need one (catalog lookups, string
//!   cleaning) are given a transient context by the caller and pass an `Mcx`
//!   explicitly here.
//! * **`Send` extra payloads.** A check hook hands its assign hook a
//!   `Box<dyn Any + Send>`; the backend-local, non-`Send` `Rc<pg_tz>` cannot
//!   travel through it, so the timezone seams traffic in the canonical zone
//!   *name* (a `String`) and re-resolve through the cached `pg_tzset`.

use ::mcx::Mcx;
use ::types_core::Oid;
use types_error::{PgResult, SqlState};

/* ---- datetime.c globals (DateStyle / DateOrder) ------------------------- */

seam_core::seam!(
    /// `int DateStyle` (datetime.c) — the current display date style.
    pub fn date_style() -> i32
);

seam_core::seam!(
    /// `int DateOrder` (datetime.c) — the current date field order.
    pub fn date_order() -> i32
);

seam_core::seam!(
    /// `assign_datestyle`'s effect: `DateStyle = style; DateOrder = order;`
    /// (datetime.c globals).
    pub fn assign_date_style(style: i32, order: i32)
);

seam_core::seam!(
    /// `ClearTimeZoneAbbrevCache()` (datetime.c) — invalidate the cached
    /// timezone-abbreviation lookups after the session zone changes.
    pub fn clear_time_zone_abbrev_cache()
);

seam_core::seam!(
    /// `load_tzoffsets(filename)` + `InstallTimeZoneAbbrevs(...)` fused
    /// (tzparser.c + datetime.c): load a timezone-abbreviation file and install
    /// the resulting table. `Ok(false)` mirrors the C NULL return (load failed,
    /// reported via `GUC_check_errmsg`); `Err` is an `ereport(ERROR)`.
    pub fn load_and_install_tz_abbrevs(filename: String) -> PgResult<bool>
);

/* ---- guc.c -------------------------------------------------------------- */

seam_core::seam!(
    /// `GetConfigOptionResetString(name)` (guc.c) — the variable's RESET value
    /// rendered as a string. `ereport(ERROR)` on an unknown/invisible option.
    pub fn get_config_option_reset_string(name: String) -> PgResult<String>
);

/* ---- timestamp.c / interval --------------------------------------------- */

seam_core::seam!(
    /// `DirectFunctionCall3(interval_in, val, InvalidOid, -1)` (timestamp.c) —
    /// parse `val` as an `interval`, returning `(month, day, time)`. `Err` is
    /// the parse `ereport(ERROR)`.
    pub fn interval_in_for_timezone(val: String) -> PgResult<(i32, i32, i64)>
);

/* ---- pgtz.c / state-pgtz (name-carrier adaptations) --------------------- */

seam_core::seam!(
    /// `pg_tzset(name)` (pgtz.c) reduced to its canonical-name result:
    /// `Some(canonical_name)` if the zone loads, `None` for an unknown zone.
    pub fn pg_tzset_name(name: String) -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `pg_tzset_offset(gmtoffset)` (pgtz.c) reduced to its canonical-name
    /// result: `Some(name)` if the fixed-offset zone is in range, else `None`.
    pub fn pg_tzset_offset_name(gmtoffset: i64) -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `pg_tz_acceptable(pg_tzset(name))` (pgtz.c / localtime.c) — re-resolve
    /// the (cached) named zone and test it does not use leap seconds.
    pub fn pg_tz_name_acceptable(name: String) -> PgResult<bool>
);

seam_core::seam!(
    /// `session_timezone = pg_tzset(name)` (pgtz.c + state-pgtz) — re-resolve
    /// the named zone and install it as the session timezone.
    pub fn set_session_timezone_by_name(name: String) -> PgResult<()>
);

seam_core::seam!(
    /// `log_timezone = pg_tzset(name)` (pgtz.c + state-pgtz).
    pub fn set_log_timezone_by_name(name: String) -> PgResult<()>
);

seam_core::seam!(
    /// `pg_get_timezone_name(session_timezone)` (pgtz.c) — the session zone's
    /// canonical name, `"unknown"` when none.
    pub fn show_session_timezone_name() -> String
);

seam_core::seam!(
    /// `pg_get_timezone_name(log_timezone)` (pgtz.c).
    pub fn show_log_timezone_name() -> String
);

/* ---- snapmgr.c ---------------------------------------------------------- */

seam_core::seam!(
    /// `bool FirstSnapshotSet` (snapmgr.c) — whether the transaction has taken
    /// its first snapshot.
    pub fn first_snapshot_set() -> bool
);

/* ---- float.c ------------------------------------------------------------ */

seam_core::seam!(
    /// `DirectFunctionCall1(setseed, Float8GetDatum(newval))` (float.c) — set
    /// the random-number seed.
    pub fn setseed(newval: f64) -> PgResult<()>
);

/* ---- encoding (encnames.c / mbutils.c) ---------------------------------- */

seam_core::seam!(
    /// `pg_valid_client_encoding(name)` (encnames.c) — encoding id for `name`,
    /// or `< 0` if not a valid client encoding.
    pub fn pg_valid_client_encoding(name: String) -> PgResult<i32>
);

seam_core::seam!(
    /// `PrepareClientEncoding(encoding)` (mbutils.c) — verify the conversion
    /// procs are reachable; `< 0` means not (yet) usable.
    pub fn prepare_client_encoding(encoding: i32) -> PgResult<i32>
);

seam_core::seam!(
    /// `SetClientEncoding(encoding)` then `elog(LOG, ...)` on a negative
    /// return (mbutils.c) — infallible from the GUC framework's perspective.
    pub fn set_client_encoding_logging(encoding: i32)
);

/* ---- miscinit.c (roles / session auth) ---------------------------------- */

seam_core::seam!(
    /// `GetSessionUserId()` (miscinit.c).
    pub fn get_session_user_id() -> Oid
);

seam_core::seam!(
    /// `GetSessionUserIsSuperuser()` (miscinit.c).
    pub fn get_session_user_is_superuser() -> bool
);

seam_core::seam!(
    /// `GetAuthenticatedUserId()` (miscinit.c).
    pub fn get_authenticated_user_id() -> Oid
);

seam_core::seam!(
    /// `GetCurrentRoleId()` (miscinit.c).
    pub fn get_current_role_id() -> Oid
);

seam_core::seam!(
    /// `bool current_role_is_superuser` (miscinit.c).
    pub fn current_role_is_superuser() -> bool
);

seam_core::seam!(
    /// `SetSessionAuthorization(roleid, is_superuser)` (miscinit.c).
    pub fn set_session_authorization(roleid: Oid, is_superuser: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `SetCurrentRoleId(roleid, is_superuser)` (miscinit.c).
    pub fn set_current_role_id(roleid: Oid, is_superuser: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `char *role_string` (guc_tables.c, the `role` GUC's storage) — the
    /// current `SET ROLE` string, or `None` when unset.
    pub fn role_string() -> Option<String>
);

/* ---- syscache (AUTHNAME role lookup) ------------------------------------ */

seam_core::seam!(
    /// `SearchSysCache1(AUTHNAME, name)` projected to `(oid, rolsuper)`
    /// (syscache.c + catalog/pg_authid). `None` when no such role.
    pub fn lookup_authid_by_name<'mcx>(
        mcx: Mcx<'mcx>,
        name: String,
    ) -> PgResult<Option<(Oid, bool)>>
);

/* ---- elog NOTICE -------------------------------------------------------- */

seam_core::seam!(
    /// `ereport(NOTICE, (errcode(code), errmsg(msg)))` — the soft PGC_S_TEST
    /// failures the role/auth check hooks raise instead of rejecting.
    pub fn notice(code: SqlState, msg: String)
);

/* ---- backend_status.c --------------------------------------------------- */

seam_core::seam!(
    /// `pgstat_report_appname(newval)` (backend_status.c).
    pub fn pgstat_report_appname(newval: String)
);

/* ---- bufmgr.c (io_combine_limit) + xlogprefetcher.c --------------------- */

seam_core::seam!(
    /// `AmStartupProcess()` (miscadmin.h) — `MyBackendType == B_STARTUP`.
    pub fn am_startup_process() -> bool
);

seam_core::seam!(
    /// `XLogPrefetchReconfigure()` (xlogprefetcher.c) — recovery prefetching
    /// must be reconfigured because a setting it depends on changed.
    pub fn xlog_prefetch_reconfigure()
);

seam_core::seam!(
    /// Recompute `io_combine_limit = Min(io_max_combine_limit,
    /// io_combine_limit_guc)` (bufmgr.c). `from_max` selects which of the two
    /// GUCs `newval` is (true = io_max_combine_limit, false = io_combine_limit).
    pub fn recompute_io_combine_limit(newval: i32, from_max: bool)
);

/* ---- octal show-hook globals -------------------------------------------- */

seam_core::seam!(
    /// `int data_directory_mode` (miscinit.c / init-small global).
    pub fn data_directory_mode() -> i32
);

seam_core::seam!(
    /// `int Log_file_mode` (syslogger.c global).
    pub fn log_file_mode() -> i32
);

seam_core::seam!(
    /// `int Unix_socket_permissions` (pqcomm.c global).
    pub fn unix_socket_permissions() -> i32
);
