//! GUC_REPORT transmission to the client (guc.c lines 2546-2670):
//! `BeginReportingGUCOptions`, `ReportChangedGUCOptions`, `ReportGUCOption`.
//!
//! These walk the **unified** process-global GUC store ([`crate::live`]) — the
//! same store the value seams read through and the `SET` write path mutates —
//! and transmit changed `GUC_REPORT` variables to the frontend as
//! `ParameterStatus` ('S') frames via the libpq byte-sink seam
//! ([`crate::seam::pq_putmessage`]).
//!
//! # Scope (honest boundary)
//!
//! `guc.c` tracks the to-report set with the `guc_report_list` slist threaded
//! through each `config_generic.report_link`. The idiomatic store does not
//! thread an intrusive list; instead `ReportChangedGUCOptions` scans the store
//! for variables with both `GUC_REPORT` (transmit at all) and `GUC_NEEDS_REPORT`
//! (changed since last report) set — exactly the predicate the C slist
//! membership encodes — and clears `GUC_NEEDS_REPORT` after transmitting. The
//! result on the wire is identical: at most one `ParameterStatus` per changed
//! reportable GUC.
//!
//! Independently of the `GUC_NEEDS_REPORT` gate, `ReportGUCOption` (guc.c:2634)
//! suppresses the `ParameterStatus` frame when the freshly-rendered value
//! equals `record->last_reported`, and refreshes `last_reported` after a
//! successful send. That per-variable de-dup is reproduced here via
//! [`needs_report`]/[`update_last_reported`] — notably it makes
//! `BeginReportingGUCOptions` (which has no `GUC_NEEDS_REPORT` gate) idempotent
//! and suppresses a redundant report when a SET re-applies the same value.
//!
//! The two `in_hot_standby` self-`SetConfigOption` hacks in the C originals
//! (which flip a GUC based on `RecoveryInProgress()`) are **not** reproduced
//! here: that path reaches the recovery machinery, an out-of-crate dependency,
//! and `in_hot_standby` is among the boot-resolved variables this store seeds
//! through the normal path. Reproducing the recovery-driven self-set is
//! documented as deferred rather than stubbed behind a pretend-success.

use types_guc::GUC_REPORT;

use crate::live::{with_store, with_store_mut};
use crate::model::GUC_NEEDS_REPORT;
use crate::registry::show_guc_option;

/// `PqMsg_ParameterStatus` (`libpq/protocol.h`) — the 'S' frame type.
const PQMSG_PARAMETER_STATUS: u8 = b'S';

/// `void BeginReportingGUCOptions(void)` (guc.c:2546).
///
/// Start automatic reporting of changes to `GUC_REPORT` variables; executed at
/// completion of backend startup. The `whereToSendOutput == DestRemote` gate is
/// the caller's (the seam is only installed on the remote-dest path), so this
/// transmits the initial values of every `GUC_REPORT` variable in the store.
///
/// Returns the number of variables transmitted (0 if the store is not yet
/// initialized — a no-op, never a panic).
pub fn begin_reporting_guc_options() -> usize {
    // Collect under an immutable borrow, transmit after dropping it (the sink
    // seam may re-enter the store via other paths). `ReportGUCOption`'s de-dup
    // against `last_reported` is applied per variable (C guc.c:2634).
    let pending: Vec<(String, String)> = with_store(|reg| {
        let mut out = Vec::new();
        for var in reg.iter() {
            if var.gen().flags & GUC_REPORT != 0 {
                let val = show_guc_option(var, false);
                if needs_report(var.gen().last_reported.as_deref(), &val) {
                    out.push((var.name_pub().into(), val));
                }
            }
        }
        out
    })
    .unwrap_or_default();

    let sent = pending.len();
    for (name, val) in &pending {
        report_guc_option(name, val);
    }
    update_last_reported(&pending);
    sent
}

/// `void ReportChangedGUCOptions(void)` (guc.c:2596).
///
/// Called just before waiting for a new client query: transmit the
/// recently-changed `GUC_REPORT` variables (those also carrying
/// `GUC_NEEDS_REPORT`) and clear their `GUC_NEEDS_REPORT` bit, so a
/// `ParameterStatus` is sent at most once per variable per query.
pub fn report_changed_guc_options() -> usize {
    let pending: Vec<(String, String)> = with_store(|reg| {
        let mut out = Vec::new();
        for var in reg.iter() {
            let flags = var.gen().flags;
            let status = var.gen().status;
            if flags & GUC_REPORT != 0 && status & GUC_NEEDS_REPORT != 0 {
                // C ReportGUCOption suppresses the frame when the rendered value
                // equals last_reported (guc.c:2638), independent of the
                // NEEDS_REPORT membership.
                let val = show_guc_option(var, false);
                if needs_report(var.gen().last_reported.as_deref(), &val) {
                    out.push((var.name_pub().into(), val));
                }
            }
        }
        out
    })
    .unwrap_or_default();

    let sent = pending.len();
    for (name, val) in &pending {
        report_guc_option(name, val);
    }

    update_last_reported(&pending);

    // Clear GUC_NEEDS_REPORT on *every* member of the to-report set, mirroring
    // the C `guc_report_list` drain (guc.c:2628): the C clears the bit and
    // unlinks each list entry whether or not `ReportGUCOption` actually
    // transmitted — so a value-equal-to-last_reported member is dropped too.
    with_store_mut(|reg| {
        for var in reg.iter_mut() {
            let g = var.gen_mut();
            if g.flags & GUC_REPORT != 0 && g.status & GUC_NEEDS_REPORT != 0 {
                g.status &= !GUC_NEEDS_REPORT;
            }
        }
    });
    sent
}

/// The transmit half of `static void ReportGUCOption(struct config_generic
/// *record)` (guc.c:2634): emit the `ParameterStatus` ('S') frame whose body is
/// the two NUL-terminated strings `name\0val\0` (C: `pq_beginmessage(
/// PqMsg_ParameterStatus); pq_sendstring(name); pq_sendstring(val);
/// pq_endmessage()`).
///
/// The de-dup against `record->last_reported` ("We need not transmit the value
/// if it's the same as what we last transmitted") and the post-send
/// `last_reported` update are applied by the callers around this call — see
/// [`needs_report`] and [`update_last_reported`] — because the byte-sink seam
/// may re-enter the store, so transmission happens after the store borrow is
/// dropped.
fn report_guc_option(name: &str, val: &str) {
    let mut body = Vec::with_capacity(name.len() + val.len() + 2);
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(val.as_bytes());
    body.push(0);
    let _ = crate::seam::pq_putmessage::call(PQMSG_PARAMETER_STATUS, &body);
}

/// C: `record->last_reported == NULL || strcmp(val, record->last_reported) != 0`
/// — whether the freshly-rendered `val` differs from what was last transmitted
/// for this variable and therefore must be sent (guc.c:2638).
fn needs_report(last_reported: Option<&str>, val: &str) -> bool {
    match last_reported {
        None => true,
        Some(prev) => prev != val,
    }
}

/// After transmitting, refresh each sent variable's `last_reported` with the
/// value just put on the wire (C: `guc_free(record->last_reported);
/// record->last_reported = guc_strdup(LOG, val);`, guc.c:2655). Runs in a
/// single mutable pass once the byte-sink seam has finished, so a re-entrant
/// store access during transmission cannot deadlock on the store borrow.
fn update_last_reported(pending: &[(String, String)]) {
    if pending.is_empty() {
        return;
    }
    with_store_mut(|reg| {
        for (name, val) in pending {
            if let Some(var) = reg.find_option_mut(name) {
                var.gen_mut().last_reported = Some(val.clone());
            }
        }
    });
}
