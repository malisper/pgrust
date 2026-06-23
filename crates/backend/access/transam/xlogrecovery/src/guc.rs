//! GUC check/assign hooks for the recovery-target / streaming parameters
//! (`check_primary_slot_name`, `check_recovery_target*` / `assign_recovery_target*`).
//!
//! The idiomatic form takes the GUC `newval` as `&str` and returns the parsed
//! "extra" value (C's `*extra`) by value through [`RecoveryTargetExtra`], rather
//! than an opaque malloc'd blob. A failed check returns `false` / `Err(())`; the
//! GUC-check error detail/hint/code are reported through the guc seams. Assign
//! hooks that detect a competing recovery target return the `ERROR` `PgError`
//! (C throws from the assign hook).
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c` (lines 4778-5105).

use alloc::format;
use alloc::string::ToString;

use ::types_core::{TimeLineID, TransactionId, XLogRecPtr};
use ::types_error::PgError;
use ::wal::MAXFNAMELEN;

use crate::core::{RecoveryTargetTimeLineGoal, RecoveryTargetType, XLogRecoveryState};

use slot_seams as slot;
use timestamp_seams as timestamp;
use guc_seams as guc;

/// The GUC "extra" value (`*extra`) computed by a `check_*` hook and consumed by
/// the matching `assign_*` hook, threaded by value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RecoveryTargetExtra {
    #[default]
    None,
    Lsn(XLogRecPtr),
    Timeline(RecoveryTargetTimeLineGoal),
    Xid(TransactionId),
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

/// `bool check_primary_slot_name(char **newval, void **extra, GucSource source)`
/// (xlogrecovery.c:4782)
pub fn check_primary_slot_name(newval: &str) -> bool {
    // if (*newval && strcmp(*newval, "") != 0 &&
    //     !ReplicationSlotValidateNameInternal(*newval, &err_code, ...))
    if !newval.is_empty() {
        if let Err((err_code, err_msg, err_hint)) =
            slot::replication_slot_validate_name_internal::call(newval)
        {
            guc::guc_check_errcode::call(err_code);
            guc::guc_check_errdetail::call(err_msg);
            if let Some(hint) = err_hint {
                guc::guc_check_errhint::call(hint);
            }
            return false;
        }
    }
    true
}

/// `pg_noreturn static void error_multiple_recovery_targets(void)`
/// (xlogrecovery.c:4821) — returns the `ERROR` `PgError` to raise.
pub(crate) fn error_multiple_recovery_targets() -> PgError {
    PgError::error("multiple recovery targets specified")
        .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
        .with_detail(
            "At most one of \"recovery_target\", \"recovery_target_lsn\", \
             \"recovery_target_name\", \"recovery_target_time\", \
             \"recovery_target_xid\" may be set.",
        )
}

/// `bool check_recovery_target(char **newval, void **extra, GucSource source)`
/// (xlogrecovery.c:4833)
pub fn check_recovery_target(newval: &str) -> bool {
    if newval != "immediate" && !newval.is_empty() {
        guc::guc_check_errdetail::call("The only allowed value is \"immediate\".".to_string());
        return false;
    }
    true
}

/// `void assign_recovery_target(const char *newval, void *extra)`
/// (xlogrecovery.c:4847)
pub fn assign_recovery_target(st: &mut XLogRecoveryState, newval: &str) -> Result<(), PgError> {
    if st.recovery_target != RecoveryTargetType::Unset
        && st.recovery_target != RecoveryTargetType::Immediate
    {
        return Err(error_multiple_recovery_targets());
    }

    if !newval.is_empty() {
        st.recovery_target = RecoveryTargetType::Immediate;
    } else {
        st.recovery_target = RecoveryTargetType::Unset;
    }
    Ok(())
}

/// `bool check_recovery_target_lsn(char **newval, void **extra, GucSource source)`
/// (xlogrecovery.c:4863) — returns `Ok(extra)` or `Err(())` on a failed check.
pub fn check_recovery_target_lsn(newval: &str) -> Result<RecoveryTargetExtra, ()> {
    if !newval.is_empty() {
        let (lsn, have_error) =
            lsn_trigfuncs::pg_lsn::pg_lsn_in_internal(newval);
        if have_error {
            return Err(());
        }
        return Ok(RecoveryTargetExtra::Lsn(lsn));
    }
    Ok(RecoveryTargetExtra::None)
}

/// `void assign_recovery_target_lsn(const char *newval, void *extra)`
/// (xlogrecovery.c:4888)
pub fn assign_recovery_target_lsn(
    st: &mut XLogRecoveryState,
    newval: &str,
    extra: RecoveryTargetExtra,
) -> Result<(), PgError> {
    if st.recovery_target != RecoveryTargetType::Unset
        && st.recovery_target != RecoveryTargetType::Lsn
    {
        return Err(error_multiple_recovery_targets());
    }

    if !newval.is_empty() {
        st.recovery_target = RecoveryTargetType::Lsn;
        if let RecoveryTargetExtra::Lsn(lsn) = extra {
            st.recovery_target_lsn = lsn;
        }
    } else {
        st.recovery_target = RecoveryTargetType::Unset;
    }
    Ok(())
}

/// `bool check_recovery_target_name(char **newval, void **extra, GucSource source)`
/// (xlogrecovery.c:4907)
pub fn check_recovery_target_name(newval: &str) -> bool {
    // if (strlen(*newval) >= MAXFNAMELEN)
    if newval.len() >= MAXFNAMELEN {
        guc::guc_check_errdetail::call(format!(
            "\"{}\" is too long (maximum {} characters).",
            "recovery_target_name",
            MAXFNAMELEN - 1
        ));
        return false;
    }
    true
}

/// `void assign_recovery_target_name(const char *newval, void *extra)`
/// (xlogrecovery.c:4923)
pub fn assign_recovery_target_name(
    st: &mut XLogRecoveryState,
    newval: &str,
) -> Result<(), PgError> {
    if st.recovery_target != RecoveryTargetType::Unset
        && st.recovery_target != RecoveryTargetType::Name
    {
        return Err(error_multiple_recovery_targets());
    }

    if !newval.is_empty() {
        st.recovery_target = RecoveryTargetType::Name;
        st.recovery_target_name = newval.into();
    } else {
        st.recovery_target = RecoveryTargetType::Unset;
    }
    Ok(())
}

/// `bool check_recovery_target_time(char **newval, void **extra, GucSource source)`
/// (xlogrecovery.c:4948)
pub fn check_recovery_target_time(newval: &str) -> bool {
    if !newval.is_empty() {
        // Reject some special values.
        if newval == "now" || newval == "today" || newval == "tomorrow" || newval == "yesterday" {
            return false;
        }

        // Parse the timestamp value (syntax-only; the time-zone-dependent final
        // parse is deferred to assign time via timestamptz_in). The seam returns
        // whether it parsed cleanly to a DTK_DATE timestamp in range.
        if !timestamp::parse_recovery_target_time::call(newval.to_string()) {
            guc::guc_check_errdetail::call(format!("Timestamp out of range: \"{}\".", newval));
            return false;
        }
    }
    true
}

/// `void assign_recovery_target_time(const char *newval, void *extra)`
/// (xlogrecovery.c:5003)
pub fn assign_recovery_target_time(
    st: &mut XLogRecoveryState,
    newval: &str,
) -> Result<(), PgError> {
    if st.recovery_target != RecoveryTargetType::Unset
        && st.recovery_target != RecoveryTargetType::Time
    {
        return Err(error_multiple_recovery_targets());
    }

    if !newval.is_empty() {
        st.recovery_target = RecoveryTargetType::Time;
    } else {
        st.recovery_target = RecoveryTargetType::Unset;
    }
    Ok(())
}

/// `bool check_recovery_target_timeline(char **newval, void **extra,`
/// `GucSource source)` (xlogrecovery.c:5019)
pub fn check_recovery_target_timeline(newval: &str) -> Result<RecoveryTargetExtra, ()> {
    let rttg: RecoveryTargetTimeLineGoal = if newval == "current" {
        RecoveryTargetTimeLineGoal::Controlfile
    } else if newval == "latest" {
        RecoveryTargetTimeLineGoal::Latest
    } else {
        // errno = 0; strtoul(*newval, NULL, 0); reject on EINVAL/ERANGE.
        let (_value, range_error) = strtoul_base0(newval.as_bytes(), u64::MAX as u128);
        if range_error {
            guc::guc_check_errdetail::call(
                "\"recovery_target_timeline\" is not a valid number.".to_string(),
            );
            return Err(());
        }
        RecoveryTargetTimeLineGoal::Numeric
    };

    Ok(RecoveryTargetExtra::Timeline(rttg))
}

/// `void assign_recovery_target_timeline(const char *newval, void *extra)`
/// (xlogrecovery.c:5054)
pub fn assign_recovery_target_timeline(
    st: &mut XLogRecoveryState,
    newval: &str,
    extra: RecoveryTargetExtra,
) {
    if let RecoveryTargetExtra::Timeline(goal) = extra {
        st.recovery_target_timeline_goal = goal;
    }
    if st.recovery_target_timeline_goal == RecoveryTargetTimeLineGoal::Numeric {
        let (value, _range_error) = strtoul_base0(newval.as_bytes(), u64::MAX as u128);
        st.recovery_target_tli_requested = value as TimeLineID;
    } else {
        st.recovery_target_tli_requested = 0;
    }
}

/// `bool check_recovery_target_xid(char **newval, void **extra, GucSource source)`
/// (xlogrecovery.c:5067)
pub fn check_recovery_target_xid(newval: &str) -> Result<RecoveryTargetExtra, ()> {
    if !newval.is_empty() {
        // errno = 0; strtou64(*newval, NULL, 0); reject on EINVAL/ERANGE.
        let (value, range_error) = strtoul_base0(newval.as_bytes(), u64::MAX as u128);
        if range_error {
            return Err(());
        }
        let xid = value as TransactionId;
        return Ok(RecoveryTargetExtra::Xid(xid));
    }
    Ok(RecoveryTargetExtra::None)
}

/// `void assign_recovery_target_xid(const char *newval, void *extra)`
/// (xlogrecovery.c:5092)
pub fn assign_recovery_target_xid(
    st: &mut XLogRecoveryState,
    newval: &str,
    extra: RecoveryTargetExtra,
) -> Result<(), PgError> {
    if st.recovery_target != RecoveryTargetType::Unset
        && st.recovery_target != RecoveryTargetType::Xid
    {
        return Err(error_multiple_recovery_targets());
    }

    if !newval.is_empty() {
        st.recovery_target = RecoveryTargetType::Xid;
        if let RecoveryTargetExtra::Xid(xid) = extra {
            st.recovery_target_xid = xid;
        }
    } else {
        st.recovery_target = RecoveryTargetType::Unset;
    }
    Ok(())
}
