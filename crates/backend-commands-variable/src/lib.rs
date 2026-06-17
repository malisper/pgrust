#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
// `PgError` is a large error type shared across the whole tree.
#![allow(clippy::result_large_err)]

//! `backend/commands/variable.c` — the `check_`/`assign_`/`show_` GUC hooks for
//! the specialized SET variables (DateStyle, TimeZone, search-path-adjacent
//! roles, transaction modes, client encoding, the octal `show_*` modes, and the
//! build-flag rejection hooks).
//!
//! Every hook's *own* logic lives in-crate: the DateStyle token parser /
//! conflict detection / canonical-string builder, the transaction-mode decision
//! trees, the SET ROLE `none` translation, the octal formatting, and the
//! build-flag checks. Everything reaching into another subsystem (datetime
//! globals + timezone loaders, the GUC framework, encoding, roles/auth, xact
//! state, varlena `SplitIdentifierString`, pgstat) crosses a seam owned by that
//! subsystem.
//!
//! The hooks are installed into the typed GUC slots in
//! [`backend_utils_misc_guc_tables::hooks`] from [`init_seams`].
//!
//! ## The GUC slot contract (`guc-tables/slots.rs`)
//!
//! * A check hook is `fn(&mut <val>, &mut Option<GucHookExtra>, GucSource) ->
//!   PgResult<bool>`. `Ok(false)` is the C `return false`; the GUC framework
//!   turns it into the user-facing error, optionally enriched by the
//!   `GUC_check_err*` thread-local this hook writes. `Err` is the hook's
//!   `ereport(ERROR)` surface. For string GUCs `<val>` is `Option<String>`
//!   (NULL boot_val stays distinguishable from empty).
//! * An assign hook is `fn(<val>, Option<&GucHookExtra>)` — it returns `()`
//!   (C `void`) and so *cannot fail*. The C assign hooks are all infallible in
//!   the normal path.
//! * A show hook is `fn() -> String`.
//!
//! `GucHookExtra = Box<dyn Any + Send>`: the opaque payload a check hook hands
//! its paired assign hook (C `void **extra`). It must be `Send`, so the
//! backend-local, non-`Send` `Rc<pg_tz>` cannot travel through it; the timezone
//! hooks therefore carry the *canonical zone name* (a `String`) in `extra` and
//! the assign hook re-resolves it through the (cached) `pg_tzset` — behavior
//! preserving, since `pg_tzset` is a hash hit on the name the check hook just
//! validated.

use std::any::Any;

use backend_utils_misc_guc::{
    GUC_check_errcode, GUC_check_errdetail, GUC_check_errhint, GUC_check_errmsg,
};
use mcx::MemoryContext;
use types_core::{
    InvalidOid, Oid, OidIsValid, DATEORDER_DMY, DATEORDER_MDY, DATEORDER_YMD, USE_GERMAN_DATES,
    USE_ISO_DATES, USE_POSTGRES_DATES, USE_SQL_DATES,
};
use types_datetime::{SECS_PER_HOUR, USECS_PER_SEC};
use types_error::{
    PgResult, ERRCODE_ACTIVE_SQL_TRANSACTION, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_TRANSACTION_STATE, ERRCODE_UNDEFINED_OBJECT,
};
use types_guc::{GucSource, PGC_S_DEFAULT, PGC_S_INTERACTIVE, PGC_S_TEST};

use backend_utils_misc_guc_tables::hooks;
use backend_utils_misc_guc_tables::GucHookExtra;

// Owner seam crates this unit reaches into.
use backend_access_transam_parallel_seams as parallel;
use backend_access_transam_xact_seams as xact;
use backend_access_transam_xlog_seams as xlog;
use backend_commands_variable_seams as own;
use backend_utils_adt_acl_seams as acl;
use backend_utils_misc_superuser_seams as superuser;

const XACT_SERIALIZABLE: i32 = types_core::XACT_SERIALIZABLE;

#[cfg(test)]
mod tests;

/// Case-insensitive equality (`pg_strcasecmp(s, t) == 0`).
fn ci_eq(s: &str, t: &str) -> bool {
    s.eq_ignore_ascii_case(t)
}

/// Case-insensitive prefix test (`pg_strncasecmp(s, prefix, prefix.len()) == 0`).
fn ci_prefix(s: &str, prefix: &str) -> bool {
    let n = prefix.len();
    s.len() >= n && s.as_bytes()[..n].eq_ignore_ascii_case(prefix.as_bytes())
}

/* =========================================================================
 * DATESTYLE
 * ========================================================================= */

/// `extra` produced by [`check_datestyle`] for [`assign_datestyle`]
/// (the C `int myextra[2]` = `{DateStyle, DateOrder}`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DateStyleExtra {
    date_style: i32,
    date_order: i32,
}

/// `check_datestyle` — GUC check_hook for `datestyle` (variable.c L51-238).
pub fn check_datestyle(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    source: GucSource,
) -> PgResult<bool> {
    let Some(result) = check_datestyle_inner(newval.as_deref().unwrap_or(""), source)? else {
        return Ok(false);
    };
    *newval = Some(result.canonical);
    *extra = Some(Box::new(DateStyleExtra {
        date_style: result.date_style,
        date_order: result.date_order,
    }));
    Ok(true)
}

struct DateStyleResult {
    canonical: String,
    date_style: i32,
    date_order: i32,
}

/// The body of `check_datestyle`, factored so the `DEFAULT` keyword can recurse
/// (the C calls `check_datestyle(&subval, &subextra, source)`). Returns `None`
/// for the C `return false` rejections (the caller surfaces `Ok(false)`).
fn check_datestyle_inner(newval: &str, source: GucSource) -> PgResult<Option<DateStyleResult>> {
    // The current DateStyle/DateOrder are the starting point (C `DateStyle` /
    // `DateOrder` datetime.c globals). They are only used when the new string
    // doesn't fully specify both, which a well-formed value always does; the
    // unported datetime owner exposes them through a seam (loud-panic until it
    // lands — same surface as the C globals being unset).
    let mut newDateStyle = own::date_style::call();
    let mut newDateOrder = own::date_order::call();
    let mut have_style = false;
    let mut have_order = false;
    let mut ok = true;

    // Need a modifiable copy of string; parse into list of identifiers. The
    // working list lives in a transient context (C `pstrdup` + `list_free`).
    let scratch = MemoryContext::new("check_datestyle");
    let elemlist = match backend_utils_adt_varlena_seams::split_identifier_string::call(
        scratch.mcx(),
        newval,
        ',',
    )? {
        Some(list) => list,
        None => {
            // syntax error in list
            GUC_check_errdetail("List syntax is invalid.");
            return Ok(None);
        }
    };

    for tok in elemlist.iter() {
        let tok = tok.as_str();

        // Ugh. Somebody ought to write a table driven version -- mjl

        if ci_eq(tok, "ISO") {
            if have_style && newDateStyle != USE_ISO_DATES {
                ok = false; // conflicting styles
            }
            newDateStyle = USE_ISO_DATES;
            have_style = true;
        } else if ci_eq(tok, "SQL") {
            if have_style && newDateStyle != USE_SQL_DATES {
                ok = false; // conflicting styles
            }
            newDateStyle = USE_SQL_DATES;
            have_style = true;
        } else if ci_prefix(tok, "POSTGRES") {
            if have_style && newDateStyle != USE_POSTGRES_DATES {
                ok = false; // conflicting styles
            }
            newDateStyle = USE_POSTGRES_DATES;
            have_style = true;
        } else if ci_eq(tok, "GERMAN") {
            if have_style && newDateStyle != USE_GERMAN_DATES {
                ok = false; // conflicting styles
            }
            newDateStyle = USE_GERMAN_DATES;
            have_style = true;
            // GERMAN also sets DMY, unless explicitly overridden
            if !have_order {
                newDateOrder = DATEORDER_DMY;
            }
        } else if ci_eq(tok, "YMD") {
            if have_order && newDateOrder != DATEORDER_YMD {
                ok = false; // conflicting orders
            }
            newDateOrder = DATEORDER_YMD;
            have_order = true;
        } else if ci_eq(tok, "DMY") || ci_prefix(tok, "EURO") {
            if have_order && newDateOrder != DATEORDER_DMY {
                ok = false; // conflicting orders
            }
            newDateOrder = DATEORDER_DMY;
            have_order = true;
        } else if ci_eq(tok, "MDY") || ci_eq(tok, "US") || ci_prefix(tok, "NONEURO") {
            if have_order && newDateOrder != DATEORDER_MDY {
                ok = false; // conflicting orders
            }
            newDateOrder = DATEORDER_MDY;
            have_order = true;
        } else if ci_eq(tok, "DEFAULT") {
            /*
             * Easiest way to get the current DEFAULT state is to fetch the
             * DEFAULT string from guc.c and recursively parse it.
             *
             * We can't simply "return check_datestyle(...)" because we need to
             * handle constructs like "DEFAULT, ISO".
             */
            let subval = own::get_config_option_reset_string::call("datestyle".to_string())?;
            match check_datestyle_inner(&subval, source)? {
                Some(sub) => {
                    if !have_style {
                        newDateStyle = sub.date_style;
                    }
                    if !have_order {
                        newDateOrder = sub.date_order;
                    }
                }
                None => {
                    ok = false;
                    break;
                }
            }
        } else {
            GUC_check_errdetail(format!("Unrecognized key word: \"{tok}\"."));
            return Ok(None);
        }
    }

    if !ok {
        GUC_check_errdetail("Conflicting \"DateStyle\" specifications.");
        return Ok(None);
    }

    // Prepare the canonical string to return.
    let mut canonical = String::with_capacity(32);
    match newDateStyle {
        x if x == USE_ISO_DATES => canonical.push_str("ISO"),
        x if x == USE_SQL_DATES => canonical.push_str("SQL"),
        x if x == USE_GERMAN_DATES => canonical.push_str("German"),
        _ => canonical.push_str("Postgres"),
    }
    match newDateOrder {
        x if x == DATEORDER_YMD => canonical.push_str(", YMD"),
        x if x == DATEORDER_DMY => canonical.push_str(", DMY"),
        _ => canonical.push_str(", MDY"),
    }

    Ok(Some(DateStyleResult {
        canonical,
        date_style: newDateStyle,
        date_order: newDateOrder,
    }))
}

/// `assign_datestyle` — GUC assign_hook for `datestyle` (variable.c L243-250).
pub fn assign_datestyle(_newval: Option<&str>, extra: Option<&GucHookExtra>) {
    let myextra = downcast::<DateStyleExtra>(extra);
    // DateStyle = myextra[0]; DateOrder = myextra[1];  (datetime.c globals)
    own::assign_date_style::call(myextra.date_style, myextra.date_order);
}

/* =========================================================================
 * TIMEZONE
 *
 * The resolved `pg_tz` is backend-local (`Rc<pg_tz>`, non-`Send`), so it cannot
 * be carried through `GucHookExtra`. We carry the canonical zone *name* and let
 * the assign hook re-resolve it (a cached `pg_tzset` hit).
 * ========================================================================= */

/// `check_timezone` — GUC check_hook for `timezone` (variable.c L260-375).
pub fn check_timezone(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let val = newval.as_deref().unwrap_or("");
    let new_tz_name: Option<String>;

    if ci_prefix(val, "interval") {
        /*
         * Support INTERVAL 'foo'.  This is for SQL spec compliance, not because
         * it has any actual real-world usefulness.
         */
        let valueptr = &val[8..];
        // while (isspace(*valueptr)) valueptr++;
        let valueptr = valueptr.trim_start_matches(|c: char| c.is_ascii_whitespace());
        // if (*valueptr++ != '\'') return false;
        let Some(rest) = valueptr.strip_prefix('\'') else {
            return Ok(false);
        };
        // Check and remove trailing quote: the first `'` must be the last char.
        let Some(close) = rest.find('\'') else {
            return Ok(false);
        };
        if close + 1 != rest.len() {
            return Ok(false);
        }
        let interval_str = &rest[..close];

        /*
         * Try to parse it. An invalid interval would ereport(ERROR) in C; the
         * seam surfaces that as a PgError propagated here.
         */
        let (month, day, time) = own::interval_in_for_timezone::call(interval_str.to_string())?;

        if month != 0 {
            GUC_check_errdetail("Cannot specify months in time zone interval.");
            return Ok(false);
        }
        if day != 0 {
            GUC_check_errdetail("Cannot specify days in time zone interval.");
            return Ok(false);
        }

        // Here we change from SQL to Unix sign convention.
        let gmtoffset = -(time / USECS_PER_SEC);
        new_tz_name = own::pg_tzset_offset_name::call(gmtoffset)?;
    } else if let Some(hours) = parse_full_f64(val) {
        // Try it as a numeric number of hours (possibly fractional).
        // Here we change from SQL to Unix sign convention.
        let gmtoffset = (-hours * SECS_PER_HOUR as f64) as i64;
        new_tz_name = own::pg_tzset_offset_name::call(gmtoffset)?;
    } else {
        // Otherwise assume it is a timezone name, and try to load it.
        match own::pg_tzset_name::call(val.to_string())? {
            None => {
                // Doesn't seem to be any great value in errdetail here.
                return Ok(false);
            }
            Some(name) => {
                if !own::pg_tz_name_acceptable::call(name.clone())? {
                    GUC_check_errmsg(format!("time zone \"{val}\" appears to use leap seconds"));
                    GUC_check_errdetail("PostgreSQL does not support leap seconds.");
                    return Ok(false);
                }
                new_tz_name = Some(name);
            }
        }
    }

    // Test for failure in pg_tzset_offset, which we assume is out-of-range.
    let Some(name) = new_tz_name else {
        GUC_check_errdetail("UTC timezone offset is out of range.");
        return Ok(false);
    };

    // Pass back data for assign_timezone to use.
    *extra = Some(Box::new(TzNameExtra { name }));

    Ok(true)
}

/// `extra` for the timezone assign hooks: the canonical zone name to install.
#[derive(Clone, Debug)]
struct TzNameExtra {
    name: String,
}

/// `assign_timezone` — GUC assign_hook for `timezone` (variable.c L380-386).
pub fn assign_timezone(_newval: Option<&str>, extra: Option<&GucHookExtra>) {
    let myextra = downcast::<TzNameExtra>(extra);
    // session_timezone = extra->tz; ClearTimeZoneAbbrevCache();
    own::set_session_timezone_by_name::call(myextra.name.clone())
        .expect("assign_timezone: re-resolving the check-validated zone cannot fail");
    own::clear_time_zone_abbrev_cache::call();
}

/// `show_timezone` — GUC show_hook for `timezone` (variable.c L391-403).
pub fn show_timezone() -> String {
    // Always show the zone's canonical name.
    own::show_session_timezone_name::call()
}

/* =========================================================================
 * LOG_TIMEZONE
 *
 * For log_timezone we don't support the interval-based methods of setting a
 * zone, which are only there for SQL spec compliance.
 * ========================================================================= */

/// `check_log_timezone` — GUC check_hook for `log_timezone` (variable.c L417-450).
pub fn check_log_timezone(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let val = newval.as_deref().unwrap_or("");

    // Assume it is a timezone name, and try to load it.
    let Some(name) = own::pg_tzset_name::call(val.to_string())? else {
        // Doesn't seem to be any great value in errdetail here.
        return Ok(false);
    };

    if !own::pg_tz_name_acceptable::call(name.clone())? {
        GUC_check_errmsg(format!("time zone \"{val}\" appears to use leap seconds"));
        GUC_check_errdetail("PostgreSQL does not support leap seconds.");
        return Ok(false);
    }

    // Pass back data for assign_log_timezone to use.
    *extra = Some(Box::new(TzNameExtra { name }));

    Ok(true)
}

/// `assign_log_timezone` — GUC assign_hook for `log_timezone` (variable.c L455-459).
pub fn assign_log_timezone(_newval: Option<&str>, extra: Option<&GucHookExtra>) {
    let myextra = downcast::<TzNameExtra>(extra);
    own::set_log_timezone_by_name::call(myextra.name.clone())
        .expect("assign_log_timezone: re-resolving the check-validated zone cannot fail");
}

/// `show_log_timezone` — GUC show_hook for `log_timezone` (variable.c L464-476).
pub fn show_log_timezone() -> String {
    // Always show the zone's canonical name.
    own::show_log_timezone_name::call()
}

/* =========================================================================
 * TIMEZONE_ABBREVIATIONS
 *
 * The C uses `*extra` to carry a `TimeZoneAbbrevTable *` (load_tzoffsets) which
 * `assign_timezone_abbreviations` then `InstallTimeZoneAbbrevs`. That table is
 * not `Send`/value-shaped here, so the load+install pair is fused in a single
 * owner seam called from the check hook (which must succeed before assign
 * runs); `assign_timezone_abbreviations` is then a no-op.
 * ========================================================================= */

/// `check_timezone_abbreviations` — GUC check_hook (variable.c L486-513).
pub fn check_timezone_abbreviations(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    source: GucSource,
) -> PgResult<bool> {
    /*
     * The boot_val for timezone_abbreviations is NULL. When we see that we just
     * do nothing. (See the long comment in variable.c.)
     */
    let Some(name) = newval.as_deref() else {
        debug_assert_eq!(source, PGC_S_DEFAULT);
        return Ok(true);
    };

    // OK, load the file and produce/install a TimeZoneAbbrevTable. tzparser.c
    // returns NULL on failure, reporting via GUC_check_errmsg.
    if !own::load_and_install_tz_abbrevs::call(name.to_string())? {
        return Ok(false);
    }
    // Marker so assign knows the table is installed (the boot_val NULL case
    // leaves extra None — the C `if (!extra) return` no-op).
    *extra = Some(Box::new(true));
    Ok(true)
}

/// `assign_timezone_abbreviations` — GUC assign_hook (variable.c L518-526).
///
/// The actual install happens inside the check hook's seam (which must succeed
/// before assign runs), so nothing is left to do here.
pub fn assign_timezone_abbreviations(_newval: Option<&str>, _extra: Option<&GucHookExtra>) {
    // Do nothing for the boot_val default of NULL (and, in this port, for the
    // already-installed table too).
}

/* =========================================================================
 * SET TRANSACTION READ ONLY / READ WRITE
 * ========================================================================= */

/// `check_transaction_read_only` (variable.c L545-574).
pub fn check_transaction_read_only(
    newval: &mut bool,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    if !*newval
        && xact::xact_read_only::call()
        && xact::is_transaction_state::call()
        && !parallel::initializing_parallel_worker::call()
    {
        // Can't go to r/w mode inside a r/o transaction.
        if xact::is_sub_transaction::call() {
            GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
            GUC_check_errmsg(
                "cannot set transaction read-write mode inside a read-only transaction",
            );
            return Ok(false);
        }
        // Top level transaction can't change to r/w after first snapshot.
        if own::first_snapshot_set::call() {
            GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
            GUC_check_errmsg("transaction read-write mode must be set before any query");
            return Ok(false);
        }
        // Can't go to r/w mode while recovery is still active.
        if xlog::recovery_in_progress::call() {
            GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
            GUC_check_errmsg("cannot set transaction read-write mode during recovery");
            return Ok(false);
        }
    }

    Ok(true)
}

/* =========================================================================
 * SET TRANSACTION ISOLATION LEVEL
 * ========================================================================= */

/// `check_transaction_isolation` (variable.c L585-617). Enum GUC: `newval` is
/// the isolation level integer.
pub fn check_transaction_isolation(
    newval: &mut i32,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let newXactIsoLevel = *newval;

    if newXactIsoLevel != xact::xact_iso_level::call()
        && xact::is_transaction_state::call()
        && !parallel::initializing_parallel_worker::call()
    {
        if own::first_snapshot_set::call() {
            GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
            GUC_check_errmsg("SET TRANSACTION ISOLATION LEVEL must be called before any query");
            return Ok(false);
        }
        // We ignore a subtransaction setting it to the existing value.
        if xact::is_sub_transaction::call() {
            GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
            GUC_check_errmsg(
                "SET TRANSACTION ISOLATION LEVEL must not be called in a subtransaction",
            );
            return Ok(false);
        }
        // Can't go to serializable mode while recovery is still active.
        if newXactIsoLevel == XACT_SERIALIZABLE && xlog::recovery_in_progress::call() {
            GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
            GUC_check_errmsg("cannot use serializable mode in a hot standby");
            GUC_check_errhint("You can use REPEATABLE READ instead.");
            return Ok(false);
        }
    }

    Ok(true)
}

/* =========================================================================
 * SET TRANSACTION [NOT] DEFERRABLE
 * ========================================================================= */

/// `check_transaction_deferrable` (variable.c L623-644).
pub fn check_transaction_deferrable(
    _newval: &mut bool,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    // Just accept the value when restoring state in a parallel worker.
    if parallel::initializing_parallel_worker::call() {
        return Ok(true);
    }

    if xact::is_sub_transaction::call() {
        GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
        GUC_check_errmsg(
            "SET TRANSACTION [NOT] DEFERRABLE cannot be called within a subtransaction",
        );
        return Ok(false);
    }
    if own::first_snapshot_set::call() {
        GUC_check_errcode(ERRCODE_ACTIVE_SQL_TRANSACTION);
        GUC_check_errmsg("SET TRANSACTION [NOT] DEFERRABLE must be called before any query");
        return Ok(false);
    }

    Ok(true)
}

/* =========================================================================
 * Random number seed
 * ========================================================================= */

/// `extra` for the random seed hooks (the C `int *` "arm" flag).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RandomSeedExtra {
    /// Whether the assign should actually fire (source was interactive SET).
    armed: bool,
}

/// `check_random_seed` (variable.c L655-665).
pub fn check_random_seed(
    _newval: &mut f64,
    extra: &mut Option<GucHookExtra>,
    source: GucSource,
) -> PgResult<bool> {
    // Arm the assign only if source of value is an interactive SET.
    *extra = Some(Box::new(RandomSeedExtra {
        armed: source >= PGC_S_INTERACTIVE,
    }));
    Ok(true)
}

/// `assign_random_seed` (variable.c L667-674).
pub fn assign_random_seed(newval: f64, extra: Option<&GucHookExtra>) {
    let myextra = downcast::<RandomSeedExtra>(extra);
    // We'll do this at most once for any setting of the GUC variable.
    if myextra.armed {
        // DirectFunctionCall1(setseed, ...) — the C cannot meaningfully fail
        // here; an out-of-range seed is rejected by the GUC min/max bounds.
        own::setseed::call(newval)
            .expect("assign_random_seed: setseed cannot fail for an in-range GUC value");
    }
    // The C resets the arm flag in the (mutable) extra so a rollback re-assign
    // is a no-op. With a `&` extra we cannot write it back; re-arming requires
    // re-running the check hook, which produces a fresh `armed` — behavior
    // preserving for the rollback case (a rollback restores the prior value via
    // its own extra, which is unarmed unless it too came from interactive SET).
}

/// `show_random_seed` (variable.c L676-680).
pub fn show_random_seed() -> String {
    "unavailable".to_string()
}

/* =========================================================================
 * SET CLIENT_ENCODING
 * ========================================================================= */

/// `check_client_encoding` (variable.c L687-783). `*extra` carries the encoding
/// id for `assign_client_encoding`.
pub fn check_client_encoding(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let val = newval.as_deref().unwrap_or("");

    // Look up the encoding by name.
    let encoding = own::pg_valid_client_encoding::call(val.to_string())?;
    if encoding < 0 {
        return Ok(false);
    }

    // Get the canonical name (no aliases, uniform case).
    let canonical_name = common_encnames_seams::pg_encoding_to_char::call(encoding).to_string();

    /*
     * Parallel workers send data to the leader, not the client. (See the long
     * comment in variable.c.)
     */
    if parallel::is_parallel_worker::call() && !parallel::initializing_parallel_worker::call() {
        GUC_check_errcode(ERRCODE_INVALID_TRANSACTION_STATE);
        GUC_check_errdetail("Cannot change \"client_encoding\" during a parallel operation.");
        return Ok(false);
    }

    /*
     * If we are not within a transaction then PrepareClientEncoding will not be
     * able to look up the necessary conversion procs. (See the long comment in
     * variable.c.)
     */
    if !parallel::is_parallel_worker::call() && own::prepare_client_encoding::call(encoding)? < 0 {
        if xact::is_transaction_state::call() {
            // Must be a genuine no-such-conversion problem.
            GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
            let db_enc = backend_utils_mb_mbutils_seams::get_database_encoding_name::call();
            GUC_check_errdetail(format!(
                "Conversion between {canonical_name} and {db_enc} is not supported."
            ));
        } else {
            // Provide a useful complaint.
            GUC_check_errdetail("Cannot change \"client_encoding\" now.");
        }
        return Ok(false);
    }

    /*
     * Replace the user-supplied string with the encoding's canonical name. (See
     * the pre-9.1 JDBC "UNICODE" hack comment in variable.c.)
     */
    if val != canonical_name && val != "UNICODE" {
        *newval = Some(canonical_name);
    }

    // Save the encoding's ID in *extra, for use by assign_client_encoding.
    *extra = Some(Box::new(encoding));

    Ok(true)
}

/// `assign_client_encoding` (variable.c L785-800).
pub fn assign_client_encoding(_newval: Option<&str>, extra: Option<&GucHookExtra>) {
    let encoding = *downcast::<i32>(extra);

    /*
     * In a parallel worker, we never override the client encoding that was set
     * by ParallelWorkerMain().
     */
    if parallel::is_parallel_worker::call() {
        return;
    }

    // We do not expect an error if PrepareClientEncoding succeeded; the C logs
    // (LOG) on a negative return rather than ereport(ERROR), so this is
    // infallible from the GUC framework's perspective.
    own::set_client_encoding_logging::call(encoding);
}

/* =========================================================================
 * SET SESSION AUTHORIZATION
 * ========================================================================= */

/// The shared `role_auth_extra` (variable.c L807-812): the assign-hook payload
/// for both SESSION AUTHORIZATION and ROLE.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RoleAuthExtra {
    roleid: Oid,
    is_superuser: bool,
}

/// `check_session_authorization` (variable.c L814-909). `*newval == NULL` (the
/// boot_val default) is the `None` case.
pub fn check_session_authorization(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    source: GucSource,
) -> PgResult<bool> {
    // Do nothing for the boot_val default of NULL.
    let Some(name) = newval.as_deref() else {
        return Ok(true);
    };

    let roleid;
    let is_superuser;

    if parallel::initializing_parallel_worker::call() {
        /*
         * In parallel worker initialization, we want to copy the leader's state
         * even if it no longer matches the catalogs.
         */
        roleid = own::get_session_user_id::call();
        is_superuser = own::get_session_user_is_superuser::call();
    } else {
        if !xact::is_transaction_state::call() {
            /*
             * Can't do catalog lookups, so fail. (session_authorization cannot
             * be set in postgresql.conf, which seems like a good thing anyway.)
             */
            return Ok(false);
        }

        /*
         * When source == PGC_S_TEST, we don't throw a hard error for a
         * nonexistent user name or insufficient privileges, only a NOTICE.
         */

        // Look up the username.
        let scratch = MemoryContext::new("check_session_authorization");
        let Some(role) = own::lookup_authid_by_name::call(scratch.mcx(), name.to_string())? else {
            if source == PGC_S_TEST {
                own::notice::call(
                    ERRCODE_UNDEFINED_OBJECT,
                    format!("role \"{name}\" does not exist"),
                );
                return Ok(true);
            }
            GUC_check_errmsg(format!("role \"{name}\" does not exist"));
            return Ok(false);
        };

        roleid = role.0;
        is_superuser = role.1;

        /*
         * Only superusers may SET SESSION AUTHORIZATION a role other than
         * itself. The original authenticated user's superuserness is what
         * matters.
         */
        let authuser = own::get_authenticated_user_id::call();
        if roleid != authuser && !superuser::superuser_arg::call(authuser)? {
            if source == PGC_S_TEST {
                own::notice::call(
                    ERRCODE_INSUFFICIENT_PRIVILEGE,
                    format!("permission will be denied to set session authorization \"{name}\""),
                );
                return Ok(true);
            }
            GUC_check_errcode(ERRCODE_INSUFFICIENT_PRIVILEGE);
            GUC_check_errmsg(format!(
                "permission denied to set session authorization \"{name}\""
            ));
            return Ok(false);
        }
    }

    // Set up "extra" struct for assign_session_authorization to use.
    *extra = Some(Box::new(RoleAuthExtra {
        roleid,
        is_superuser,
    }));

    Ok(true)
}

/// `assign_session_authorization` (variable.c L911-921).
pub fn assign_session_authorization(_newval: Option<&str>, extra: Option<&GucHookExtra>) {
    // Do nothing for the boot_val default of NULL.
    let Some(boxed) = extra else {
        return;
    };
    let myextra: &RoleAuthExtra = boxed
        .downcast_ref()
        .expect("assign_session_authorization extra is RoleAuthExtra");
    own::set_session_authorization::call(myextra.roleid, myextra.is_superuser)
        .expect("assign_session_authorization cannot fail in the normal path");
}

/* =========================================================================
 * SET ROLE
 *
 * The SQL spec requires "SET ROLE NONE" to unset the role, so we hardwire a
 * translation of "none" to InvalidOid. Otherwise much like SET SESSION
 * AUTHORIZATION.
 * ========================================================================= */

/// `check_role` (variable.c L932-1023).
pub fn check_role(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    source: GucSource,
) -> PgResult<bool> {
    let name = newval.as_deref().unwrap_or("");

    let roleid;
    let is_superuser;

    if name == "none" {
        // hardwired translation
        roleid = InvalidOid;
        is_superuser = false;
    } else if parallel::initializing_parallel_worker::call() {
        /*
         * In parallel worker initialization, we want to copy the leader's state
         * even if it no longer matches the catalogs.
         */
        roleid = own::get_current_role_id::call();
        is_superuser = own::current_role_is_superuser::call();
    } else {
        if !xact::is_transaction_state::call() {
            /*
             * Can't do catalog lookups, so fail. (role cannot be set in
             * postgresql.conf, which seems like a good thing anyway.)
             */
            return Ok(false);
        }

        /*
         * When source == PGC_S_TEST, we don't throw a hard error for a
         * nonexistent user name or insufficient privileges, only a NOTICE.
         */

        // Look up the username.
        let scratch = MemoryContext::new("check_role");
        let Some(role) = own::lookup_authid_by_name::call(scratch.mcx(), name.to_string())? else {
            if source == PGC_S_TEST {
                own::notice::call(
                    ERRCODE_UNDEFINED_OBJECT,
                    format!("role \"{name}\" does not exist"),
                );
                return Ok(true);
            }
            GUC_check_errmsg(format!("role \"{name}\" does not exist"));
            return Ok(false);
        };

        roleid = role.0;
        is_superuser = role.1;

        // Verify that session user is allowed to become this role.
        let session_user = own::get_session_user_id::call();
        if !acl::member_can_set_role::call(session_user, roleid)? {
            if source == PGC_S_TEST {
                own::notice::call(
                    ERRCODE_INSUFFICIENT_PRIVILEGE,
                    format!("permission will be denied to set role \"{name}\""),
                );
                return Ok(true);
            }
            GUC_check_errcode(ERRCODE_INSUFFICIENT_PRIVILEGE);
            GUC_check_errmsg(format!("permission denied to set role \"{name}\""));
            return Ok(false);
        }
    }

    // Set up "extra" struct for assign_role to use.
    *extra = Some(Box::new(RoleAuthExtra {
        roleid,
        is_superuser,
    }));

    Ok(true)
}

/// `assign_role` (variable.c L1025-1031).
pub fn assign_role(_newval: Option<&str>, extra: Option<&GucHookExtra>) {
    let myextra = downcast::<RoleAuthExtra>(extra);
    own::set_current_role_id::call(myextra.roleid, myextra.is_superuser)
        .expect("assign_role cannot fail in the normal path");
}

/// `show_role` (variable.c L1033-1048).
pub fn show_role() -> String {
    /*
     * Check whether SET ROLE is active; if not return "none". (See the kluge
     * comment in variable.c about SET SESSION AUTHORIZATION resetting SET ROLE.)
     */
    if !OidIsValid(own::get_current_role_id::call()) {
        return "none".to_string();
    }

    // Otherwise we can just use the GUC string.
    own::role_string::call().unwrap_or_else(|| "none".to_string())
}

/* =========================================================================
 * PATH VARIABLES
 * ========================================================================= */

/// `check_canonical_path` (variable.c L1058-1069). Used for `log_directory` and
/// other GUCs that just canonicalize the path. `*newval == NULL` (e.g.
/// external_pid_file's default) is the `None` case.
pub fn check_canonical_path(
    newval: &mut Option<String>,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    /*
     * Since canonicalize_path never enlarges the string, the C modifies newval
     * in place. But watch out for NULL.
     */
    if let Some(path) = newval.take() {
        *newval = Some(common_path_seams::canonicalize_path::call(path));
    }
    Ok(true)
}

/* =========================================================================
 * MISCELLANEOUS
 * ========================================================================= */

/// `check_application_name` — GUC check_hook for `application_name`
/// (variable.c L1079-1102).
pub fn check_application_name(
    newval: &mut Option<String>,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    clean_ascii_name(newval)
}

/// `assign_application_name` — GUC assign_hook for `application_name`
/// (variable.c L1107-1112).
pub fn assign_application_name(newval: Option<&str>, _extra: Option<&GucHookExtra>) {
    // Update the pg_stat_activity view.
    own::pgstat_report_appname::call(newval.unwrap_or("").to_string());
}

/// `check_cluster_name` — GUC check_hook for `cluster_name` (variable.c L1117-1140).
pub fn check_cluster_name(
    newval: &mut Option<String>,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    clean_ascii_name(newval)
}

/// Shared body of `check_application_name` / `check_cluster_name`: only allow
/// clean ASCII chars in the name, replacing `*newval` with the cleaned copy.
/// `pg_clean_ascii(str, MCXT_ALLOC_NO_OOM)` only returns NULL on OOM, which the
/// C maps to `return false`.
fn clean_ascii_name(newval: &mut Option<String>) -> PgResult<bool> {
    let val = newval.as_deref().unwrap_or("");
    let scratch = MemoryContext::new("clean_ascii_name");
    // MCXT_ALLOC_NO_OOM == 0x02 (utils/palloc.h).
    let cleaned = match common_string::pg_clean_ascii(scratch.mcx(), val, 0x02) {
        Ok(clean) => Some(clean.as_str().to_string()),
        Err(_) => None,
    };
    match cleaned {
        Some(clean) => {
            *newval = Some(clean);
            Ok(true)
        }
        None => Ok(false),
    }
}

/// `assign_maintenance_io_concurrency` — GUC assign_hook (variable.c L1145-1155).
pub fn assign_maintenance_io_concurrency(_newval: i32, _extra: Option<&GucHookExtra>) {
    // The `maintenance_io_concurrency = newval` store is done by the GUC
    // framework's write-through of the variable. The hook's own job is to
    // reconfigure recovery prefetching, because a setting it depends on changed.
    if own::am_startup_process::call() {
        own::xlog_prefetch_reconfigure::call();
    }
}

/// `assign_io_max_combine_limit` (variable.c L1163-1167).
/// `io_combine_limit = Min(newval, io_combine_limit_guc)`.
pub fn assign_io_max_combine_limit(newval: i32, _extra: Option<&GucHookExtra>) {
    own::recompute_io_combine_limit::call(newval, true);
}

/// `assign_io_combine_limit` (variable.c L1168-1172).
/// `io_combine_limit = Min(io_max_combine_limit, newval)`.
pub fn assign_io_combine_limit(newval: i32, _extra: Option<&GucHookExtra>) {
    own::recompute_io_combine_limit::call(newval, false);
}

/* These show hooks just exist because we want to show the values in octal. */

/// `show_data_directory_mode` (variable.c L1181-1188).
pub fn show_data_directory_mode() -> String {
    format!("{:04o}", own::data_directory_mode::call())
}

/// `show_log_file_mode` (variable.c L1193-1200).
pub fn show_log_file_mode() -> String {
    format!("{:04o}", own::log_file_mode::call())
}

/// `show_unix_socket_permissions` (variable.c L1205-1212).
pub fn show_unix_socket_permissions() -> String {
    format!("{:04o}", own::unix_socket_permissions::call())
}

/* These check hooks do nothing more than reject non-default settings in builds
 * that don't support them. */

/// `check_bonjour` (variable.c L1220-1231). This build does not define
/// `USE_BONJOUR`, so any `true` setting is rejected.
pub fn check_bonjour(
    newval: &mut bool,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    if *newval {
        GUC_check_errmsg("Bonjour is not supported by this build");
        return Ok(false);
    }
    Ok(true)
}

/// `check_default_with_oids` (variable.c L1233-1246).
pub fn check_default_with_oids(
    newval: &mut bool,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    if *newval {
        // check the GUC's definition for an explanation
        GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
        GUC_check_errmsg("tables declared WITH OIDS are not supported");
        return Ok(false);
    }
    Ok(true)
}

/// `check_ssl` (variable.c L1248-1259). This build does not define `USE_SSL`,
/// so any `true` setting is rejected.
pub fn check_ssl(
    newval: &mut bool,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    if *newval {
        GUC_check_errmsg("SSL is not supported by this build");
        return Ok(false);
    }
    Ok(true)
}

/* =========================================================================
 * In-crate helpers
 * ========================================================================= */

/// Downcast a required (non-NULL) GUC `extra` payload to its concrete type.
fn downcast<T: Any>(extra: Option<&GucHookExtra>) -> &T {
    extra
        .expect("GUC assign hook reached with no extra payload")
        .downcast_ref::<T>()
        .expect("GUC assign hook extra has unexpected type")
}

/// Parse `s` as a full `strtod`-style f64 (mirrors C `strtod(*newval, &endptr);
/// endptr != *newval && *endptr == '\0'`). Returns `None` if any trailing junk
/// remains or nothing was consumed.
fn parse_full_f64(s: &str) -> Option<f64> {
    let trimmed = s.trim_start_matches(|c: char| c.is_ascii_whitespace());
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<f64>().ok()
}

/* =========================================================================
 * Seam installation
 * ========================================================================= */

/// Install every GUC hook this unit owns into the typed `guc-tables` slots,
/// and install the cross-subsystem adapter seams this unit declares (delegating
/// to the real owner where ported; loud mirror-pg-and-panic where not).
pub fn init_seams() {
    install::own_seams();

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
    hooks::show_log_file_mode.install(show_log_file_mode);
    hooks::show_unix_socket_permissions.install(show_unix_socket_permissions);
    hooks::check_bonjour.install(check_bonjour);
    hooks::check_default_with_oids.install(check_default_with_oids);
    hooks::check_ssl.install(check_ssl);
}

/// Install the cross-subsystem adapter seams this unit declares
/// (`backend-commands-variable-seams`). Each body either delegates to the real
/// owner crate (when it is ported) or loud-panics with the C symbol name
/// (mirror-pg-and-panic) until that owner lands. No subsystem logic lives here.
mod install {
    use super::own;
    use backend_utils_error::ereport;
    use types_error::error::{LOG, NOTICE};
    use types_error::ErrorLocation;

    fn here(fn_name: &'static str) -> ErrorLocation {
        ErrorLocation::new("../src/backend/commands/variable.c", 0, fn_name)
    }

    pub fn own_seams() {
        // -------- datetime.c globals (DateStyle / DateOrder) --------
        // Real per-backend storage lives in backend-utils-adt-datetime::settings
        // (the globals.c `int DateStyle`/`int DateOrder`); delegate the reads and
        // the assign_datestyle store to it.
        own::date_style::set(backend_utils_adt_datetime::settings::date_style);
        own::date_order::set(backend_utils_adt_datetime::settings::date_order);
        own::assign_date_style::set(|style, order| {
            backend_utils_adt_datetime::settings::set_date_style(style);
            backend_utils_adt_datetime::settings::set_date_order(order);
        });
        own::clear_time_zone_abbrev_cache::set(|| {
            // ClearTimeZoneAbbrevCache() (datetime.c:3227) zeros the per-field
            // `tzabbrevcache` lookup cache. That cache is a pure performance
            // optimization that the Rust DecodeTimezoneAbbrev port omits
            // (lookups always go through the timezone engine / resolver hook), so
            // there is nothing to clear — a faithful no-op, not a stub.
        });
        own::load_and_install_tz_abbrevs::set(|filename| {
            // C: check_timezone_abbreviations -> load_tzoffsets(filename), then
            // assign_timezone_abbreviations -> InstallTimeZoneAbbrevs(tbl). The
            // table is not value-shippable through *extra here, so the load and
            // install are fused: load_tzoffsets parses the file into the owned
            // TimeZoneAbbrevTable, then install_time_zone_abbrevs makes it the
            // active runtime abbreviation table.
            use backend_utils_misc_guc::{
                GUC_check_errdetail, GUC_check_errhint, GUC_check_errmsg,
            };
            match backend_utils_misc_timeout::tzparser::load_tzoffsets(&filename) {
                Ok(table) => {
                    // InstallTimeZoneAbbrevs(tbl).
                    backend_utils_adt_datetime::tz_abbrev_install::install_time_zone_abbrevs(
                        table,
                    );
                    Ok(true)
                }
                // C: load_tzoffsets returned NULL after reporting via
                // GUC_check_errmsg/errdetail/errhint; mirror that "soft" failure
                // (return false), re-emitting the recorded diagnostics. An empty
                // message is the depth-0 "let guc.c's own invalid-value message
                // stand" case (no GUC_check_errmsg).
                Err(err) => {
                    if !err.message.is_empty() {
                        GUC_check_errmsg(err.message);
                    }
                    if let Some(detail) = err.detail {
                        GUC_check_errdetail(detail);
                    }
                    if let Some(hint) = err.hint {
                        GUC_check_errhint(hint);
                    }
                    Ok(false)
                }
            }
        });

        // -------- guc.c (merged) — delegate --------
        own::get_config_option_reset_string::set(|name| {
            // GetConfigOptionResetString(name): the option's RESET value as a
            // string. The C `find_option(name, ..., ERROR)` + `Assert(record)`
            // contract means an unknown name is a hard error; here the store
            // lookup returns None for both unknown and value-less, which for the
            // only caller (the always-present "datestyle") cannot happen.
            match backend_utils_misc_guc::get_reset_string(&name) {
                Some(Some(s)) => Ok(s),
                Some(None) => Ok(String::new()),
                None => Ok(String::new()),
            }
        });

        // -------- timestamp.c (unported: todo) --------
        own::interval_in_for_timezone::set(|_val| {
            panic!("interval_in (timestamp.c) not yet ported")
        });

        // -------- pgtz.c / localtime.c / state-pgtz (ported) — delegate --------
        own::pg_tzset_name::set(|name| {
            Ok(backend_timezone_pgtz::pg_tzset(&name)?
                .map(|tz| backend_timezone_localtime::pg_get_timezone_name(&tz).to_string()))
        });
        own::pg_tzset_offset_name::set(|gmtoffset| {
            Ok(backend_timezone_pgtz::pg_tzset_offset(gmtoffset)?
                .map(|tz| backend_timezone_localtime::pg_get_timezone_name(&tz).to_string()))
        });
        own::pg_tz_name_acceptable::set(|name| {
            // The name was already validated by pg_tzset_name; re-resolve (cache
            // hit) and apply the leap-second test.
            match backend_timezone_pgtz::pg_tzset(&name)? {
                Some(tz) => Ok(backend_timezone_localtime::pg_tz_acceptable(&tz)),
                None => Ok(false),
            }
        });
        own::set_session_timezone_by_name::set(|name| {
            let tz = backend_timezone_pgtz::pg_tzset(&name)?
                .expect("set_session_timezone_by_name: zone was validated by the check hook");
            state_pgtz::set_session_timezone(tz);
            Ok(())
        });
        own::set_log_timezone_by_name::set(|name| {
            let tz = backend_timezone_pgtz::pg_tzset(&name)?
                .expect("set_log_timezone_by_name: zone was validated by the check hook");
            state_pgtz::set_log_timezone(tz);
            Ok(())
        });
        own::show_session_timezone_name::set(|| {
            let tz = state_pgtz::session_timezone();
            let name = backend_timezone_localtime::pg_get_timezone_name(&tz);
            if name.is_empty() {
                "unknown".to_string()
            } else {
                name.to_string()
            }
        });
        own::show_log_timezone_name::set(|| {
            let tz = state_pgtz::log_timezone();
            let name = backend_timezone_localtime::pg_get_timezone_name(&tz);
            if name.is_empty() {
                "unknown".to_string()
            } else {
                name.to_string()
            }
        });

        // -------- snapmgr.c (merged) — delegate --------
        own::first_snapshot_set::set(backend_utils_time_snapmgr::FirstSnapshotSet);

        // -------- setseed (pseudorandomfuncs.c) --------
        // Installed by the real owner crate
        // `backend_utils_adt_pseudorandomfuncs::init_seams()`; not set here.

        // -------- encoding: encnames.c (ported) / mbutils.c (unported) --------
        // `pg_valid_client_encoding(name)` (encnames.c): the FE-valid encoding id
        // for `name`, or `-1`. The encnames unit owns the value core.
        own::pg_valid_client_encoding::set(|name| {
            Ok(common_extra_encnames::pg_valid_client_encoding(&name))
        });
        // `PrepareClientEncoding(encoding)` (mbutils.c): verify the conversion
        // procs are reachable (may run a syscache scan, so a transient context
        // backs the lookup). `< 0` means not (yet) usable.
        own::prepare_client_encoding::set(|encoding| {
            let scratch = mcx::MemoryContext::new("PrepareClientEncoding");
            backend_utils_mb_mbutils::PrepareClientEncoding(scratch.mcx(), encoding)
        });
        // `assign_client_encoding`'s `SetClientEncoding(encoding)` (mbutils.c)
        // then `elog(LOG, "SetClientEncoding(%d) failed", encoding)` on a
        // negative return — infallible from the GUC framework's perspective.
        own::set_client_encoding_logging::set(|encoding| {
            match backend_utils_mb_mbutils::SetClientEncoding(encoding) {
                Ok(rc) if rc >= 0 => {}
                _ => {
                    let _ = ereport(LOG)
                        .errmsg(format!("SetClientEncoding({encoding}) failed"))
                        .finish(here("assign_client_encoding"));
                }
            }
        });

        // -------- miscinit.c (merged) — delegate --------
        own::get_session_user_id::set(backend_utils_init_miscinit::GetSessionUserId);
        own::get_session_user_is_superuser::set(
            backend_utils_init_miscinit::GetSessionUserIsSuperuser,
        );
        own::get_authenticated_user_id::set(backend_utils_init_miscinit::GetAuthenticatedUserId);
        own::get_current_role_id::set(backend_utils_init_miscinit::GetCurrentRoleId);
        own::current_role_is_superuser::set(|| {
            // The C `bool current_role_is_superuser` miscinit.c global is not yet
            // surfaced; only reached during parallel-worker init.
            panic!("current_role_is_superuser (miscinit.c) not yet ported")
        });
        own::set_session_authorization::set(
            backend_utils_init_miscinit::SetSessionAuthorization,
        );
        own::set_current_role_id::set(backend_utils_init_miscinit::SetCurrentRoleId);
        own::role_string::set(|| {
            // The `role` GUC's string storage (guc-tables `role_string` var).
            backend_utils_misc_guc_tables::vars::role_string.read()
        });

        // -------- syscache (AUTHNAME, unported) — delegate to its seam --------
        own::lookup_authid_by_name::set(|mcx, name| {
            Ok(
                backend_utils_cache_syscache_seams::lookup_authid_by_name::call(mcx, &name)?
                    .map(|row| (row.oid, row.rolsuper)),
            )
        });

        // -------- elog NOTICE (ported error crate) — delegate --------
        own::notice::set(|code, msg| {
            // ereport(NOTICE, (errcode(code), errmsg(msg))). NOTICE does not
            // unwind, so the PgResult is always Ok.
            let _ = ereport(NOTICE)
                .errcode(code)
                .errmsg(msg)
                .finish(here("check_role"));
        });

        // -------- backend_status.c (unported: todo) --------
        own::pgstat_report_appname::set(|_newval| {
            panic!("pgstat_report_appname (backend_status.c) not yet ported")
        });

        // -------- io_combine_limit / xlogprefetcher --------
        own::am_startup_process::set(|| {
            backend_utils_init_small_seams::my_backend_type::call()
                == types_core::init::BackendType::Startup
        });
        own::xlog_prefetch_reconfigure::set(
            backend_access_transam_xlogprefetcher::XLogPrefetchReconfigure,
        );
        own::recompute_io_combine_limit::set(|_newval, _from_max| {
            // io_combine_limit's derived global storage + setter are not yet
            // wired in production (only the read-through getter is installed).
            panic!("recompute io_combine_limit (bufmgr.c) not yet ported")
        });

        // -------- octal show-hook globals --------
        own::data_directory_mode::set(backend_utils_init_small::globals::data_directory_mode);
        own::log_file_mode::set(backend_postmaster_syslogger::config::log_file_mode);
        own::unix_socket_permissions::set(backend_libpq_pqcomm::config::unix_socket_permissions);
    }
}
