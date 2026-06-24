//! GUC check/assign hooks for the recovery-target / streaming parameters
//! (`check_primary_slot_name`, `check_recovery_target*` / `assign_recovery_target*`).
//!
//! These match the GUC machinery's `GucStringCheckFn` / `GucStringAssignFn`
//! function-pointer shapes (`guc_tables::slots`) so they install directly onto
//! the `guc_tables::hooks` slots. A check hook may canonicalize `*newval` and
//! produce the `*extra` payload its paired assign hook consumes; `Ok(false)` is
//! the C `return false` rejection. The assign hooks write the recovery-target
//! file-static globals (mirrored in [`crate::gucvars`]); `InitWalRecovery`
//! snapshots them into the startup process's `XLogRecoveryState`.
//!
//! A conflicting recovery target is C's `error_multiple_recovery_targets()`,
//! `ereport(ERROR)` from the assign hook. Because the assign-hook signature is
//! `void`-returning (C's), and these are all `PGC_POSTMASTER` GUCs whose hooks
//! only fire during postmaster startup, the conflict surfaces here as
//! `ereport(FATAL)` (errfinish emits the message and `proc_exit`s, exactly the C
//! "abort postmaster startup" outcome).
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c` (lines 4778-5105).

use alloc::boxed::Box;
use alloc::format;
use alloc::string::{String, ToString};

use ::types_core::{TimeLineID, TransactionId, XLogRecPtr};
use ::types_error::{PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use ::types_guc::GucSource;
use ::wal::MAXFNAMELEN;

use ::guc_tables::GucHookExtra;
use ::utils_error::ereport;

use crate::core::{RecoveryTargetTimeLineGoal, RecoveryTargetType};
use crate::gucvars;

use slot_seams as slot;
use timestamp_seams as timestamp;
use guc_seams as guc;

#[inline]
fn loc(lineno: i32, func: &'static str) -> ::types_error::ErrorLocation {
    ::types_error::ErrorLocation::new("xlogrecovery.c", lineno, func)
}

/// `strtoul(str, NULL, 0)` / `strtou64(str, NULL, 0)` with base auto-detection,
/// returning `(value, range_error)`. Mirrors the libc semantics the C hooks
/// rely on (0x → base 16, leading 0 → base 8, else base 10; wrapping negate).
fn strtoul_base0(s: &[u8], max: u128) -> (u128, bool) {
    let mut index = 0usize;

    while matches!(
        s.get(index),
        Some(b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
    ) {
        index += 1;
    }

    let mut neg = false;
    if s.get(index) == Some(&b'-') {
        neg = true;
        index += 1;
    } else if s.get(index) == Some(&b'+') {
        index += 1;
    }

    let base: u128 = match (s.get(index), s.get(index + 1)) {
        (Some(b'0'), Some(b'x' | b'X')) => {
            index += 2;
            16
        }
        (Some(b'0'), _) => 8,
        _ => 10,
    };

    let modulus = max + 1;
    let mut value: u128 = 0;
    let mut range_error = false;
    while let Some(&byte) = s.get(index) {
        let digit = match byte {
            b'0'..=b'9' => (byte - b'0') as u128,
            b'a'..=b'f' => (byte - b'a' + 10) as u128,
            b'A'..=b'F' => (byte - b'A' + 10) as u128,
            _ => break,
        };
        if digit >= base {
            break;
        }
        value = value * base + digit;
        if value > max {
            range_error = true;
            value = max;
        }
        index += 1;
    }

    if neg && value != 0 {
        value = modulus - value;
    }

    (value, range_error)
}

/// `pg_noreturn static void error_multiple_recovery_targets(void)`
/// (xlogrecovery.c:4821) — C `ereport(ERROR)` from the assign hook. Realized as
/// `FATAL` here (assign hooks are void; these GUCs are `PGC_POSTMASTER`, so the
/// hook only runs at startup, where an ERROR aborts the postmaster anyway).
fn error_multiple_recovery_targets() -> ! {
    let _ = ereport(::types_error::FATAL)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg("multiple recovery targets specified")
        .errdetail(
            "At most one of \"recovery_target\", \"recovery_target_lsn\", \
             \"recovery_target_name\", \"recovery_target_time\", \
             \"recovery_target_xid\" may be set.",
        )
        .finish(loc(4823, "error_multiple_recovery_targets"));
    // `finish` at FATAL never returns (errfinish proc_exits). If a sink override
    // ever lets it return, abort loudly rather than silently continue.
    unreachable!("FATAL ereport returned in error_multiple_recovery_targets")
}

/// `bool check_primary_slot_name(char **newval, void **extra, GucSource source)`
/// (xlogrecovery.c:4782)
pub fn check_primary_slot_name(
    newval: &mut Option<String>,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    // if (*newval && strcmp(*newval, "") != 0 &&
    //     !ReplicationSlotValidateNameInternal(*newval, &err_code, ...))
    let val = newval.as_deref().unwrap_or("");
    if !val.is_empty() {
        if let Err((err_code, err_msg, err_hint)) =
            slot::replication_slot_validate_name_internal::call(val)
        {
            guc::guc_check_errcode::call(err_code);
            guc::guc_check_errdetail::call(err_msg);
            if let Some(hint) = err_hint {
                guc::guc_check_errhint::call(hint);
            }
            return Ok(false);
        }
    }
    Ok(true)
}

/// `bool check_recovery_target(char **newval, void **extra, GucSource source)`
/// (xlogrecovery.c:4833)
pub fn check_recovery_target(
    newval: &mut Option<String>,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let val = newval.as_deref().unwrap_or("");
    if val != "immediate" && !val.is_empty() {
        guc::guc_check_errdetail::call("The only allowed value is \"immediate\".".to_string());
        return Ok(false);
    }
    Ok(true)
}

/// `void assign_recovery_target(const char *newval, void *extra)`
/// (xlogrecovery.c:4847)
pub fn assign_recovery_target(newval: Option<&str>, _extra: Option<&GucHookExtra>) {
    let cur = gucvars::recovery_target();
    if cur != RecoveryTargetType::Unset && cur != RecoveryTargetType::Immediate {
        error_multiple_recovery_targets();
    }

    if matches!(newval, Some(v) if !v.is_empty()) {
        gucvars::set_recovery_target(RecoveryTargetType::Immediate);
    } else {
        gucvars::set_recovery_target(RecoveryTargetType::Unset);
    }
}

/// `bool check_recovery_target_lsn(char **newval, void **extra, GucSource source)`
/// (xlogrecovery.c:4863)
pub fn check_recovery_target_lsn(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let val = newval.as_deref().unwrap_or("");
    if !val.is_empty() {
        let (lsn, have_error) = lsn_trigfuncs::pg_lsn::pg_lsn_in_internal(val);
        if have_error {
            return Ok(false);
        }
        *extra = Some(Box::new(lsn));
    }
    Ok(true)
}

/// `void assign_recovery_target_lsn(const char *newval, void *extra)`
/// (xlogrecovery.c:4888)
pub fn assign_recovery_target_lsn(newval: Option<&str>, extra: Option<&GucHookExtra>) {
    let cur = gucvars::recovery_target();
    if cur != RecoveryTargetType::Unset && cur != RecoveryTargetType::Lsn {
        error_multiple_recovery_targets();
    }

    if matches!(newval, Some(v) if !v.is_empty()) {
        gucvars::set_recovery_target(RecoveryTargetType::Lsn);
        if let Some(lsn) = extra.and_then(|e| e.downcast_ref::<XLogRecPtr>()) {
            gucvars::set_recovery_target_lsn(*lsn);
        }
    } else {
        gucvars::set_recovery_target(RecoveryTargetType::Unset);
    }
}

/// `bool check_recovery_target_name(char **newval, void **extra, GucSource source)`
/// (xlogrecovery.c:4907)
pub fn check_recovery_target_name(
    newval: &mut Option<String>,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    // if (strlen(*newval) >= MAXFNAMELEN)
    let val = newval.as_deref().unwrap_or("");
    if val.len() >= MAXFNAMELEN {
        guc::guc_check_errdetail::call(format!(
            "\"{}\" is too long (maximum {} characters).",
            "recovery_target_name",
            MAXFNAMELEN - 1
        ));
        return Ok(false);
    }
    Ok(true)
}

/// `void assign_recovery_target_name(const char *newval, void *extra)`
/// (xlogrecovery.c:4923)
pub fn assign_recovery_target_name(newval: Option<&str>, _extra: Option<&GucHookExtra>) {
    let cur = gucvars::recovery_target();
    if cur != RecoveryTargetType::Unset && cur != RecoveryTargetType::Name {
        error_multiple_recovery_targets();
    }

    if let Some(v) = newval.filter(|v| !v.is_empty()) {
        gucvars::set_recovery_target(RecoveryTargetType::Name);
        gucvars::set_recovery_target_name(v.to_string());
    } else {
        gucvars::set_recovery_target(RecoveryTargetType::Unset);
    }
}

/// `bool check_recovery_target_time(char **newval, void **extra, GucSource source)`
/// (xlogrecovery.c:4948)
pub fn check_recovery_target_time(
    newval: &mut Option<String>,
    _extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let val = newval.as_deref().unwrap_or("");
    if !val.is_empty() {
        // Reject some special values.
        if val == "now" || val == "today" || val == "tomorrow" || val == "yesterday" {
            return Ok(false);
        }

        // Parse the timestamp value (syntax-only; the time-zone-dependent final
        // parse is deferred to validate time via timestamptz_in). The seam
        // returns whether it parsed cleanly to a DTK_DATE timestamp in range.
        if !timestamp::parse_recovery_target_time::call(val.to_string()) {
            guc::guc_check_errdetail::call(format!("Timestamp out of range: \"{}\".", val));
            return Ok(false);
        }
    }
    Ok(true)
}

/// `void assign_recovery_target_time(const char *newval, void *extra)`
/// (xlogrecovery.c:5003)
pub fn assign_recovery_target_time(newval: Option<&str>, _extra: Option<&GucHookExtra>) {
    let cur = gucvars::recovery_target();
    if cur != RecoveryTargetType::Unset && cur != RecoveryTargetType::Time {
        error_multiple_recovery_targets();
    }

    if matches!(newval, Some(v) if !v.is_empty()) {
        gucvars::set_recovery_target(RecoveryTargetType::Time);
    } else {
        gucvars::set_recovery_target(RecoveryTargetType::Unset);
    }
}

/// `bool check_recovery_target_timeline(char **newval, void **extra,`
/// `GucSource source)` (xlogrecovery.c:5019)
pub fn check_recovery_target_timeline(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let val = newval.as_deref().unwrap_or("");
    let rttg: RecoveryTargetTimeLineGoal = if val == "current" {
        RecoveryTargetTimeLineGoal::Controlfile
    } else if val == "latest" {
        RecoveryTargetTimeLineGoal::Latest
    } else {
        // errno = 0; strtoul(*newval, NULL, 0); reject on EINVAL/ERANGE.
        let (_value, range_error) = strtoul_base0(val.as_bytes(), u64::MAX as u128);
        if range_error {
            guc::guc_check_errdetail::call(
                "\"recovery_target_timeline\" is not a valid number.".to_string(),
            );
            return Ok(false);
        }
        RecoveryTargetTimeLineGoal::Numeric
    };

    *extra = Some(Box::new(rttg));
    Ok(true)
}

/// `void assign_recovery_target_timeline(const char *newval, void *extra)`
/// (xlogrecovery.c:5054)
pub fn assign_recovery_target_timeline(newval: Option<&str>, extra: Option<&GucHookExtra>) {
    if let Some(goal) = extra.and_then(|e| e.downcast_ref::<RecoveryTargetTimeLineGoal>()) {
        gucvars::set_recovery_target_timeline_goal(*goal);
    }
    if gucvars::recovery_target_timeline_goal() == RecoveryTargetTimeLineGoal::Numeric {
        let (value, _range_error) =
            strtoul_base0(newval.unwrap_or("").as_bytes(), u64::MAX as u128);
        gucvars::set_recovery_target_tli_requested(value as TimeLineID);
    } else {
        gucvars::set_recovery_target_tli_requested(0);
    }
}

/// `bool check_recovery_target_xid(char **newval, void **extra, GucSource source)`
/// (xlogrecovery.c:5067)
pub fn check_recovery_target_xid(
    newval: &mut Option<String>,
    extra: &mut Option<GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let val = newval.as_deref().unwrap_or("");
    if !val.is_empty() {
        // errno = 0; strtou64(*newval, NULL, 0); reject on EINVAL/ERANGE.
        let (value, range_error) = strtoul_base0(val.as_bytes(), u64::MAX as u128);
        if range_error {
            return Ok(false);
        }
        let xid = value as TransactionId;
        *extra = Some(Box::new(xid));
    }
    Ok(true)
}

/// `void assign_recovery_target_xid(const char *newval, void *extra)`
/// (xlogrecovery.c:5092)
pub fn assign_recovery_target_xid(newval: Option<&str>, extra: Option<&GucHookExtra>) {
    let cur = gucvars::recovery_target();
    if cur != RecoveryTargetType::Unset && cur != RecoveryTargetType::Xid {
        error_multiple_recovery_targets();
    }

    if matches!(newval, Some(v) if !v.is_empty()) {
        gucvars::set_recovery_target(RecoveryTargetType::Xid);
        if let Some(xid) = extra.and_then(|e| e.downcast_ref::<TransactionId>()) {
            gucvars::set_recovery_target_xid(*xid);
        }
    } else {
        gucvars::set_recovery_target(RecoveryTargetType::Unset);
    }
}
